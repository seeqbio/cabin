from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_obo


class DiseaseOntologyOfficial(ExternalFile):
    version = 'v2020-08-21'

    @property
    def url(self):
        # release: https://github.com/DiseaseOntology/HumanDiseaseOntology/releases
        return 'https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/{v}/src/ontology/HumanDO.obo'.format(v=self.version)


class DiseaseOntologyFile(LocalFile):
    version = '1'
    depends = [DiseaseOntologyOfficial]
    extension = 'obo'


class DiseaseOntologyTable(RecordByRecordImportedTable):
    version = '1'
    depends = [DiseaseOntologyFile]

    columns = ['name', 'id', 'children', 'parents', 'omim_ids', 'mesh_ids']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
            name       VARCHAR(255)  NOT NULL,        -- human readable disease name
            id         VARCHAR(255)  PRIMARY KEY,     -- unique identifier, eg: DOID:0060318
            parents    VARCHAR(255)  NOT NULL,        -- pipe separated immediate parents
            children   VARCHAR(5180) NOT NULL,        -- pipe separated immediate children
            omim_ids   VARCHAR(5180) NOT NULL,        -- xrefs to Online Mendelian Inheritance in Man
            mesh_ids   VARCHAR(255)  NOT NULL         -- xrefs to Medical Subject Headings
        );
        """

    def read(self):
        for term in read_obo(str(self.input.path)):
            yield {
                'id': term['id'],
                'name': term['name'],
                'children': '|'.join(term['children']),
                'parents': '|'.join(term['parents']),
                'omim_ids': '|'.join(xref for xref in term['xrefs'] if xref.startswith('OMIM:')),
                'mesh_ids': '|'.join(xref for xref in term['xrefs'] if xref.startswith('MESH:'))
            }
