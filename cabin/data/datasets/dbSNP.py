from biodb import logger
from biodb.io import read_vcf
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile


class dbSNPOfficial(ExternalFile):
    version = '150'

    @property
    def url(self):
        return 'ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606_b{version}_GRCh38p7/VCF/00-All.vcf.gz'.format(version=self.version)


class dbSNPFile(LocalFile):
    version = '1'
    depends = [dbSNPOfficial]
    extension = 'txt'


class dbSNPTable(RecordByRecordImportedTable):
    version = ' 1'
    depends = [dbSNPFile]

    columns = [
        'gene_symbol',
        'gene_id',
        'chromosome',
        'pos',
        'ref',
        'alt',
        'id_dbSNP',
        'CAF',
        'TOPMed',
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
              gene_symbol     VARCHAR(255) NOT NULL,        -- HGNC gene symbol
              gene_id         VARCHAR(255) NOT NULL,        -- NCBI gene identifier (integer stored as string)
              chromosome      CHAR(2) NOT NULL,             -- chromosome as string (could be X, Y, or MT)
              pos             VARCHAR(255) NOT NULL,        -- genomic position
              ref             VARCHAR(255) NOT NULL,        -- reference genome sequence
              alt             VARCHAR(255) NOT NULL,        -- alternative allele sequence
              id_dbSNP        VARCHAR(255) NOT NULL,        -- dbSNP identifier (rs...)
              CAF             VARCHAR(255) NOT NULL,        -- 1000 genome MAF as string, '.' for null
              TOPMed          VARCHAR(255) NOT NULL,        -- TOPMed MAF as string, '.' for null
              INDEX (chromosome, pos, ref, alt)
            )
        """

    def read(self):
        # MAF fields (CAF and TOPMED), when present, have 1 + number of
        # alternative alleles (the first value is for the major, ref, # allele)
        for row in read_vcf(self.input.path):
            alts = row['ALT']
            if not row['ALT']:  # eg: rs1085307652 has ALT ='.'
                continue
            cafs = row['INFO'].get('CAF', tuple())
            topmed_afs = row['INFO'].get('TOPMED', tuple())

            # we should either not have a particular frequency (CAF or TOPMed)
            # or if we do we must have one more than the number of alternative
            # alleles, add unknown values (".") when no AF is given
            if len(cafs) == 0:
                cafs = ['.'] * len(alts)
            else:
                if len(cafs) != len(alts) + 1:
                    logger.info('Expected number of CAF values to be 1 + number of alternative alleles, skipping ' + row['ID'])
                # ignore the major (ref) allele frequency
                cafs = cafs[1:]

            if len(topmed_afs) == 0:
                topmed_afs = ['.'] * len(alts)
            else:
                if len(topmed_afs) != len(alts) + 1:
                    logger.info('Expected number of TOPMED values to be 1 + number of alternative alleles, skipping ' + row['ID'])
                # ignore the major (ref) allele frequency
                topmed_afs = topmed_afs[1:]
                topmed_afs = tuple(af if af else '.' for af in topmed_afs)  # replace None with '.', eg: rs201268514

            # GENEINFO may be multivalued (e.g. rs397508104)
            for info in row['INFO'].get('GENEINFO', '').split('|'):
                if not info:
                    continue
                gene_symbol, gene_id = info.split(':')
                for alt, caf, topmed_af in zip(alts, cafs, topmed_afs):
                    yield {
                        'chromosome': row['CHROM'],
                        'pos': row['POS'],
                        'id_dbSNP': row['ID'],
                        'ref': row['REF'],
                        'alt': alt,
                        'TOPMed': topmed_af,
                        'CAF': caf,
                        'gene_symbol': gene_symbol,
                        'gene_id': gene_id
                    }
