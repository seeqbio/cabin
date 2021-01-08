from tempfile import NamedTemporaryFile
from biodb.io import gunzip
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class medgenNamesOfficial(FTPTimestampedFile):
    version = '2020-12-23'
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    ftp_path = '/pub/medgen/NAMES.RRF.gz'


class medgenNamesS3Mirror(S3MirrorFile):
    version = '1'
    depends = [medgenNamesOfficial]
    extension = 'gz'


class medgenNamesFile(S3MirroredLocalFile):
    version = '1'
    depends = [medgenNamesS3Mirror]
    extension = 'gz'


class medgenNamesTable(RecordByRecordImportedTable):
    version = '1'
    depends = [medgenNamesFile]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                CUI 	               VARCHAR(225),        -- concept unique identifier, medgen id: C0279094
                name	               VARCHAR(225),        -- eg, Adult Acute Myeloid Leukemia in Remission
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
