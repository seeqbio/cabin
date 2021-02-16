from biodb.data.files import LocalFile, ExternalFile
from biodb.data.db import ImportedTable
from biodb.io import gunzip
from tempfile import NamedTemporaryFile


class PfamOfficial(ExternalFile):
    version = '33.1'

    @property
    def url(self):
        return 'ftp://ftp.ebi.ac.uk/pub/databases/Pfam/releases/Pfam{version}/database_files/pfamA.txt.gz'.format(version=self.version)


class PfamFile(LocalFile):
    version = '1'
    depends = [PfamOfficial]
    extension = 'txt.gz'


class PfamTable(ImportedTable):
    version = '1'
    depends = [PfamFile]
    tags = ['active']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                pfam_acc VARCHAR(255) PRIMARY KEY,
                description VARCHAR(255) NOT NULL,
                comment_ TEXT
            );
        """

    def import_table(self, cursor):
        with NamedTemporaryFile('w') as temp:
            gunzip(self.input.path, temp.name)
            cursor.execute("""
                LOAD DATA LOCAL INFILE '{path}'
                INTO TABLE `{table}`
            """.format(path=temp.name, table=self.table_name))
