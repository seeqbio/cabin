from biodb.data.db import ImportedTable
from biodb.data.files import LocalFile, ExternalFile


class ClinVarVCFOfficial(ExternalFile):
    version = '2020-04'

    @property
    def url(self):
        return 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar_{v}.vcf.gz'.format(v=self.version)


class ClinVarVCFFile(LocalFile):
    version = '1'
    depends = ClinVarVCFOfficial
    extension = 'vcf.gz'


class ClinVarVCFTable(ImportedTable):
    version = '15'
    depends = ClinVarVCFFile

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
              variation_id          INTEGER      NOT NULL,
              accession             VARCHAR(255) NOT NULL,
              ...
            );
        """

    def read(self):
        # mock
        print('Reading from ' + self.input.path)
        yield

    def produce(self):
        self.create_table()
        # mock
        for row in self.read():
            print('INSERT INTO {tbl} VALUES (...)\n'.format(tbl=self.table_name))
