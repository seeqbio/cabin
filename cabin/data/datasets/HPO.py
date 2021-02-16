from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_obo


class HPOOfficial(ExternalFile):
    version = 'v2020-08-11'

    @property
    def url(self):
        # releases: https://github.com/obophenotype/human-phenotype-ontology/releases
        return 'https://raw.githubusercontent.com/obophenotype/human-phenotype-ontology/{v}/hp.obo'.format(v=self.version)


class HPOFile(LocalFile):
    version = '1'
    depends = [HPOOfficial]
    extension = 'obo'


class HPOTable(RecordByRecordImportedTable):
    version = '1'
    depends = [HPOFile]
    tags = ['active']

    columns = ['name', 'id', 'children', 'parents', 'do_ids']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
            name       VARCHAR(255)  NOT NULL,        -- human readable disease name
            id         VARCHAR(255)  PRIMARY KEY,     -- unique identifier, eg: DOID:0060318
            parents    VARCHAR(255)  NOT NULL,        -- pipe separated immediate parents
            children   VARCHAR(5180) NOT NULL,        -- pipe separated immediate children
            do_ids     VARCHAR(255)  NOT NULL      -- xrefs to Disease Ontology
        );
        """

    def read(self):
        for term in read_obo(str(self.input.path)):
            yield {
                'id': term['id'],
                'name': term['name'],
                'children': '|'.join(term['children']),
                'parents': '|'.join(term['parents']),
                'do_ids': '|'.join(xref for xref in term['xrefs'] if xref.startswith('DOID:'))
            }
