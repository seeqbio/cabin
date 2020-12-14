from biodb.data.db import RecordByRecordImportMixin
from biodb.data.db import ImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_xsv
from biodb import settings
from pathlib import Path

class TestDatasetOfficial(ExternalFile):
    version = 'human.head'

    @property
    def url(self):
        return 'https://gist.githubusercontent.com/amirkdv/' \
               '85e91edf4b032ec5460177c44c36b446/raw/0ae6efdf95ecfb62b7a534bafa324120a7119779/gene2refseq_{v}'.format(v=self.version)


class TestDatasetFile(LocalFile):
    version = '1'
    depends = {'TestDatasetOfficial': TestDatasetOfficial}
    extension = 'txt'


class TestDatasetTable(RecordByRecordImportMixin, ImportedTable):
    version = '10'
    depends = {'TestDatasetFile': TestDatasetFile}

    columns = [
        'GeneID',
        'Symbol',
    ]

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                GeneID                                         VARCHAR(255) NOT NULL,    -- NCBI gene identifier (integer stored as string)
                Symbol                                         VARCHAR(255) NOT NULL,    -- HGNC gene symbol
                INDEX (Symbol)
            );
        """

    def read(self):
        for row in read_xsv(self.inputs['TestDatasetFile'].path):
            yield row
