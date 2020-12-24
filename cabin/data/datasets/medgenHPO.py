from tempfile import NamedTemporaryFile
from biodb.io import gunzip
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class medgenHPOOfficial(FTPTimestampedFile):
    version = '2020-12-23'
    ftp_server = 'ftp.ncbi.nlm.nih.gov'
    ftp_path = '/pub/medgen/MedGen_HPO_Mapping.txt.gz'  # Note: despite .txt, file is still RRF, ie: `|` seperated


class medgenHPOS3Mirror(S3MirrorFile):
    version = '1'
    depends = [medgenHPOOfficial]
    extension = 'gz'


class medgenHPOFile(S3MirroredLocalFile):
    version = '1'
    depends = [medgenHPOS3Mirror]
    extension = 'gz'


class medgenHPOTable(RecordByRecordImportedTable):
    version = ' 1'
    depends = [medgenHPOFile]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                CUI 	               VARCHAR(225),      -- concept unique identifier. medgen id eg, C0266399
                SDUI	               VARCHAR(225),      -- hpo id eg, HP:0000013
                HpoStr	               VARCHAR(225),      -- hpo name (string)
                MedGenStr	               VARCHAR(225),      -- medgen perfered term (string)
                MedGenStr_SAB              VARCHAR(225),      -- source of the term in medgen
                STY	                       VARCHAR(225),      -- semantic type eg, Disease or Syndrome, Finding, Anatomical Abnormality, ..
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
