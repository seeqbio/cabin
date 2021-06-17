from biodb.io import read_xsv
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class GeneInfoOfficial(FTPTimestampedFile):
    version = '2021-03-08'
    ftp_server = 'ftp.ncbi.nih.gov'
    ftp_path = '/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz'


class GeneInfoS3Mirror(S3MirrorFile):
    version = '1'
    depends = [GeneInfoOfficial]
    extension = 'gz'


class GeneInfoFile(S3MirroredLocalFile):
    version = '1'
    depends = [GeneInfoS3Mirror]
    extension = 'gz'


class GeneInfoTable(RecordByRecordImportedTable):
    version = '1'
    depends = [GeneInfoFile]
    tags = ['active']

    columns = [
        'gene_id',
        'gene_symbol',
        'synonyms',
        'chromosome',
        'map_location',
        'description',
        'other_designations',
    ]
    field_mappings = {
        'GeneID': 'gene_id',
        'Symbol': 'gene_symbol',
        'Synonyms': 'synonyms',
        'chromosome': 'chromosome',
        'map_location': 'map_location',
        'description': 'description',
        'Other_designations': 'other_designations',
    }

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                gene_id                     VARCHAR(255) NOT NULL UNIQUE,
                gene_symbol                 VARCHAR(255) NOT NULL,  -- gene symbol is not necessarily unique
                                                                    -- e.g. MT copy of nuclear gene (e.g. RNR1)
                synonyms                    VARCHAR(255),
                chromosome                  VARCHAR(255),
                map_location                VARCHAR(255),
                description                 VARCHAR(255),
                other_designations          LONG VARCHAR,
                INDEX (gene_symbol),
                INDEX (gene_id)
            );
        """

    def read(self):
        for row in read_xsv(self.input.path, gzipped=True):
            yield row


class GeneSynonymsTable(RecordByRecordImportedTable):
    version = '1'
    depends = [GeneInfoFile]
    tags = ['active']

    columns = [
        'gene_id',
        'gene_symbol',
        'canonical_symbol',
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                gene_id                     VARCHAR(255) NOT NULL,  -- eg: 4536
                gene_symbol                 VARCHAR(255) NOT NULL,  -- eg: MTND2
                canonical_symbol            VARCHAR(255) NOT NULL,  -- eg: ND2
                INDEX (canonical_symbol),
                INDEX (gene_id)
            );
        """

    def read(self):
        for row in read_xsv(self.input.path, gzipped=True):
            seen_entries = set()
            for synonym in row['Synonyms'].split('|'):
                entry = (row['GeneID'], synonym, row['Symbol'])
                if entry not in seen_entries:
                    seen_entries.add(entry)
                    yield dict(zip(self.columns, entry))
