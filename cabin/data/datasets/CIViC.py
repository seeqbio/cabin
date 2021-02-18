from pyliftover import LiftOver

from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_xsv
from biodb import settings
from biodb import logger


class CIViCOfficial(ExternalFile):
    version = '01-Nov-2020'

    @property
    def url(self):
        return 'https://civicdb.org/downloads/{version}/{version}-ClinicalEvidenceSummaries.tsv'.format(version=self.version)


class CIViCFile(LocalFile):
    version = '1'
    depends = [CIViCOfficial]
    extension = 'tsv'


class CIViCTable(RecordByRecordImportedTable):
    version = '1'
    depends = [CIViCFile]

    columns = [
        'gene',
        'variant',
        'chromosome_37',
        'start_37',
        'stop_37',
        'chromosome_38',
        'start_38',
        'reference_bases',
        'disease',
        'drugs',
        'drug_interaction_type',
        'phenotypes',
        'evidence_level',
        'nct_ids',
        'rating',
        'evidence_direction',
        'clinical_significance',
        'evidence_statement',
        'representative_transcript',
        'ensembl_version',
        'reference_build',
        'evidence_type',
        'source_type',
        'gene_civic_url',
        'entrez_id',
        'do_id'
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                gene                       VARCHAR(225) NOT NULL,             -- for more documentation, see https://docs.civicdb.org/en/latest/model/evidence/overview.html
                variant                    VARCHAR(225) NOT NULL,
                chromosome_37              VARCHAR(225) NOT NULL,
                start_37                   VARCHAR(225) NOT NULL,
                stop_37                    VARCHAR(225) NOT NULL,
                chromosome_38              VARCHAR(225) DEFAULT '' NOT NULL,
                start_38                   VARCHAR(225) DEFAULT '' NOT NULL,
                reference_bases            VARCHAR(225) NOT NULL,
                disease                    VARCHAR(225) NOT NULL,
                drugs                      VARCHAR(225) NOT NULL,              -- `,` separated drug name, if multiple then drug_interaction_type is obligatory
                                                                               -- NOTE a single drug value might still have a `,` in its name, e.g. `2,4-pyrimidinediamine` !
                drug_interaction_type      VARCHAR(225),                       -- if multiple drugs this describe their interaction
                phenotypes                 VARCHAR(225),                       -- optional, HPO phenotype associated, esp. for cases with >1 disease for genotype
                evidence_level             VARCHAR(225),                       -- A:validated (phase3), B:clinical, C:case study, D:pre-clinical (in-vitro, mouse), E:inferential
                nct_ids                    VARCHAR(225),                       -- NCI thesaurus id
                rating                     VARCHAR(225),                       -- Aggregate score 1-5 of curator's confidence in evidence (5=strong)
                evidence_direction         VARCHAR(225) NOT NULL,
                clinical_significance      VARCHAR(225) NOT NULL,
                evidence_statement         LONGTEXT     CHARACTER SET utf8mb4,
                representative_transcript  VARCHAR(225) NOT NULL,
                ensembl_version            VARCHAR(225) NOT NULL,
                reference_build            VARCHAR(225) NOT NULL,
                evidence_type              VARCHAR(225) NOT NULL,
                source_type                VARCHAR(225) NOT NULL,
                gene_civic_url             VARCHAR(225) NOT NULL,
                entrez_id                  VARCHAR(225) NOT NULL,
                do_id                      VARCHAR(225) NOT NULL,              -- Disease Ontology id, :eg: 12603
                INDEX (chromosome_37,start_37,reference_bases)
            );
        """

    def read(self):
        chains_dir = settings.SGX_ROOT_DIR / 'biodb/data/chainfiles'
        lo_to19 = LiftOver(str(chains_dir) + '/b37tohg19.chain')
        lo_to38 = LiftOver(str(chains_dir) + '/hg19tohg38.chain')

        for row in read_xsv(self.input.path, header_leading_hash=False, encoding='utf-8'):
            if not row['chromosome'] or not row['start']:  # it can have a start but not chr, ex: VHL-L158fs (c.473insT)
                logger.info('Skipping {gene}-{variant}: no genomic coordinates'.format(gene=row['gene'], variant=row['variant']))
                continue
            hg19_coor = lo_to19.convert_coordinate(row['chromosome'], int(row['start']))
            hg38_coor = lo_to38.convert_coordinate(hg19_coor[0][0], hg19_coor[0][1])

            if not hg38_coor:  # skip if liftover fails, ex: NCOA2 chr8:80
                logger.info('Skipping {gene}-{variant}: failed to liftover to GRCh38'.format(gene=row['gene'], variant=row['variant']))
                continue
            row['chromosome_37'] = row.pop('chromosome')
            row['start_37'] = row.pop('start')
            row['stop_37'] = row.pop('stop')
            row['chromosome_38'] = hg38_coor[0][0]
            row['start_38'] = str(hg38_coor[0][1])
            if row['reference_build']:  # ex: TYMS-RS34743033, lacks the string but coordinates are correct for GRCh37
                assert row['reference_build'] == 'GRCh37', "Reference is % and not GRCh37, needs reassessment!" % row['reference_build']

            row['do_id'] = 'DOID:' + row['doid']

            yield row

