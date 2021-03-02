import os
import zipfile

from biodb import logger
from biodb.data.db import ImportedTable
from biodb.data.files import (
    ExternalFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class dbNSFPOfficial(ExternalFile):
    version = '3.5c'

    @property
    def url(self):
        # FIXME currently using version 3.5c from *our mirrors* on s3
        # source version is higher with new data columns, will require logic update.
        return 'ftp://dbnsfp:dbnsfp@dbnsfp.softgenetics.com/dbNSFPv{version}.zip'.format(version=self.version)


class dbNSFPS3Mirror(S3MirrorFile):
    version = '1'
    depends = [dbNSFPOfficial]
    extension = 'zip'


class dbNSFPFile(S3MirroredLocalFile):
    version = '1'
    depends = [dbNSFPS3Mirror]
    extension = 'zip'


class dbNSFPTable(ImportedTable):
    version = '1'
    depends = [dbNSFPFile]
    tags = ['active']

    columns = [
        'chr',
        'pos(1-based)',
        'ref',
        'alt',
        'rs_dbSNP150',
        'genename',
        'clinvar_clnsig',
        'SIFT_pred',
        'FATHMM_pred',
        'MutationTaster_pred',
        'SIFT_score',
        'FATHMM_score',
        'MutationTaster_score',
        '1000Gp3_AF',
        'ExAC_AF',
        'gnomAD_exomes_AF',
        'gnomAD_genomes_AF',
        'LRT_score',
        'MutationAssessor_score',
        'LoFtool_score',
        'MetaSVM_score',
        'MetaLR_score',
        'MutPred_score',
        'fathmm-MKL_coding_score',
        'GenoCanyon_score',
        'integrated_fitCons_score',
        'GM12878_fitCons_score',
        'H1-hESC_fitCons_score',
        'HUVEC_fitCons_score',
        'phyloP100way_vertebrate',
        'phyloP20way_mammalian',
        'phastCons100way_vertebrate',
        'phastCons20way_mammalian',
    ]

    @property
    def schema(self):
    	return """
        CREATE TABLE `{table}` (
            chr                          CHAR(2)      NOT NULL,
            `pos(1-based)`               INTEGER      NOT NULL,
            ref                          VARCHAR(255) NOT NULL,
            alt                          VARCHAR(255) NOT NULL,
            rs_dbSNP150                  VARCHAR(255),
            genename                     VARCHAR(255),         -- HGNC gene symbol
            -- Allele frequency fields
            1000Gp3_AF                   FLOAT,
            ExAC_AF                      FLOAT,
            gnomAD_exomes_AF             FLOAT,
            gnomAD_genomes_AF            FLOAT,
            -- clinical impact predictions, see dbNSFP readme for
            -- (a) conversion between codes/integers and impact predictions
            -- (b) delimiters for multiple predictions
            clinvar_clnsig               VARCHAR(255),
            SIFT_pred                    VARCHAR(255),
            FATHMM_pred                  VARCHAR(255),
            MutationTaster_pred          VARCHAR(255),
            SIFT_score                   VARCHAR(255),         -- `;` separated list of floats kept as string
            FATHMM_score                 VARCHAR(255),         -- `;` separated list of floats kept as string
            MutationTaster_score         VARCHAR(255),         -- `;` separated list of floats kept as string
            LRT_score                    FLOAT,
            MutationAssessor_score       FLOAT,
            LoFtool_score                FLOAT,                -- predictor, larger = damaging
            MetaSVM_score                FLOAT,                -- ensembl method, larger = damaging
            MetaLR_score                 FLOAT,                -- ensembl method, larger = damaging
            MutPred_score                FLOAT,                -- predictor, larger = damaging
            `fathmm-MKL_coding_score`    FLOAT,                -- predictor, larger = damaging
            GenoCanyon_score             FLOAT,                -- predictor, larger = damaging
            -- concervation scores, larger = more conserved = likely damaging
            integrated_fitCons_score     FLOAT,                -- integration (ie: highly correlated, not indep.) of: GM12878, H1-hESC and HUVEC
            GM12878_fitCons_score        FLOAT,                -- fitCons of cell type, GM12878
            `H1-hESC_fitCons_score`      FLOAT,                -- fitCons of cell type, H1-hESC
            HUVEC_fitCons_score          FLOAT,                -- fitCons of cell type, HUVEC
            phyloP100way_vertebrate      FLOAT,
            phyloP20way_mammalian        FLOAT,
            phastCons100way_vertebrate   FLOAT,
            phastCons20way_mammalian     FLOAT,
            INDEX (chr, `pos(1-based)`, ref, alt)
        )
        """

    def import_table(self, cursor):
        extract_dir = self.input.path
        dbzipped = zipfile.ZipFile(extract_dir)

        # cf. ensembl for more on LOAD DATA
        # cf. https://dev.mysql.com/doc/refman/8.0/en/load-data.html

        query_tpl = """
            LOAD DATA LOCAL INFILE '{path}'
            INTO TABLE `{table}`
            IGNORE 1 LINES
            ({cols})
            SET {set_nulls}
        """
        for chrom in [str(x) for x in range(1, 23)] + ['M', 'X', 'Y']:
            logger.info('Processing dbNSFP data for chromsome "%s"' % chrom)
            chr_sub = 'dbNSFP{v}_variant.chr{c}'.format(v=self.root_versions()[0], c=chrom)
            path = dbzipped.extract(chr_sub)
            with open(path) as f:
                header = f.readline().strip()[1:].split()
            sql_cols = []
            for col in header:
                if col in self.columns:
                    # create a variable using @ so we can use our SET
                    # clause to convert "." to NULL
                    sql_cols.append('@`%s`' % col)
                else:
                    # dummy variable to skip columns that we don't want
                    sql_cols.append('@dummy')

            set_nulls = ', '.join(
                "`{col}`=NULLIF(@`{col}`, '.')".format(col=col)
                for col in self.columns
            )
            query = query_tpl.format(
                path=path,
                table=self.table_name,
                cols=', '.join(sql_cols),
                set_nulls=set_nulls
            )
            cursor.execute(query)
            os.remove(path)
        dbzipped.close()
