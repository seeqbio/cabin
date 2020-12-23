from tempfile import NamedTemporaryFile
from biodb.io import gunzip
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class medgenDefnOfficial(FTPTimestampedFile):
    version = '2020-12-23'
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    ftp_path = '/pub/medgen/MGDEF.RRF.gz'


class medgenDefnS3Mirror(S3MirrorFile):
    version = '1'
    depends = [medgenDefnOfficial]
    extension = 'gz'


class medgenDefnFile(S3MirroredLocalFile):
    version = '1'
    depends = [medgenDefnS3Mirror]
    extension = 'gz'


class medgenDefnTable(RecordByRecordImportedTable):
    """ Medgen definitions.
        Some odd RFF formating for sentences, as-is import with `\r`, affecting terminal display. (eg: C0001529)
        for more format information, see https://ftp.ncbi.nlm.nih.gov/pub/medgen/README.txt
        and https://www.ncbi.nlm.nih.gov/books/NBK9685/"""
    version = ' 1'
    depends = [medgenDefnFile]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                CUI 	               VARCHAR(225),        -- concept unique identifier. medgen id eg, C0266399
                DEF 	               VARCHAR(5180),       -- concept defintion, Caution seperators, see note in import or medgen readme: https://ftp.ncbi.nlm.nih.gov/pub/medgen/README.txt
                                                                -- USER OF DEF BEWARE! strip whitespace! see sgx comment https://gitlab.com/streamlinegenomics/biodb/-/merge_requests/42#note_450232478
                source	               VARCHAR(225),        -- eg: NCI. Alternatively: SNOMEDCT_US, GTR, MSH
                SUPPRESS	               VARCHAR(225),        -- Suppressed by UMLS curators (N only?)
                INDEX (CUI)
            );
    """

    def import_table(self, cursor):
        with NamedTemporaryFile('w') as temp:
            gunzip(self.input.path, temp.name)
            cursor.execute("""
                LOAD DATA LOCAL INFILE '{path}'
                INTO TABLE `{table}`
                FIELDS TERMINATED BY '|'
                IGNORE 1 LINES;
            """.format(path=temp.name, table=self.table_name))
