from biodb import logger
from biodb.io import cut_tsv_with_zcat
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile


class dbSNPOfficial(ExternalFile):
    version = '150'

    @property
    def url(self):
        return 'ftp://ftp.ncbi.nih.gov/snp/organisms/human_9606_b{version}_GRCh38p7/VCF/00-All.vcf.gz'.format(version=self.version)


class dbSNPzipFile(LocalFile):
    version = '1'
    depends = [dbSNPOfficial]
    extension = 'vcf.gz'


class dbSNPPartialFile(LocalFile):
    version = '1'
    depends = [dbSNPzipFile]
    extension = 'tsv'

    def produce(self):
        cut_tsv_with_zcat(self.input.path, self.path)


class dbSNPTable(RecordByRecordImportedTable):
    version = '1'
    depends = [dbSNPPartialFile]
    tags = ['active']

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
              chromosome      CHAR(2) NOT NULL,             -- chromosome as string (could be X, Y, or MT)
              pos             VARCHAR(255) NOT NULL,        -- genomic position
              id_dbSNP        VARCHAR(255) NOT NULL,        -- dbSNP identifier (rs...)
              ref             VARCHAR(255) NOT NULL,        -- reference genome sequence
              alt             VARCHAR(255) NOT NULL,        -- alternative allele sequence
              INFO            VARCHAR(255) NOT NULL,        -- INFO field
              INDEX (chromosome, pos, ref, alt)
            );
        """

    def import_table(self, cursor):
        cursor.execute("""
            LOAD DATA LOCAL INFILE '{path}'
            INTO TABLE `{table}`
            IGNORE 57 LINES
            (chromosome, pos, id_dbSNP, ref, alt, @dummy, @dummy, INFO);
        """.format(path=self.input.path, table=self.table_name))
