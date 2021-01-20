from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_obo


class MondoOfficial(ExternalFile):
    version = 'v2020-09-14'

    @property
    def url(self):
        # release: https://github.com/monarch-initiative/mondo/releases
        return 'https://github.com/monarch-initiative/mondo/releases/download/{v}/mondo.obo'.format(v=self.version)


class MondoFile(LocalFile):
    version = '1'
    depends = [MondoOfficial]
    extension = 'obo'


class MondoTable(RecordByRecordImportedTable):
    version = '1'
    depends = [MondoFile]

    columns = ['name', 'id', 'children', 'parents', 'hpo_ids', 'do_ids', 'omim_ids', 'mesh_ids']

    @property
    def schema(self):
        return """
          CREATE TABLE `{table}` (
            name       VARCHAR(255)   NOT NULL,         -- human readable disease name
            id         VARCHAR(255)   PRIMARY KEY,      -- unique identifier, eg: MONDO:0004355
            parents    VARCHAR(255)   NOT NULL,         -- pipe separated immediate parent
            children   VARCHAR(32768) NOT NULL,         -- pipe separated immediate children
            hpo_ids    VARCHAR(255)   NOT NULL,         -- xrefs to Human Phenotype Ontology
            do_ids     VARCHAR(255)   NOT NULL,         -- xrefs to Disease Ontology
            omim_ids   VARCHAR(255)   NOT NULL,         -- xrefs to Online Mendelian Inheritance in Man
            mesh_ids   VARCHAR(255)   NOT NULL          -- xrefs to Medical Subject Headings
        );
        """

    def read(self):
        for term in read_obo(str(self.input.path)):
            yield {
                'id': term['id'],
                'name': term['name'],
                'children': '|'.join(term['children']),
                'parents': '|'.join(term['parents']),
                'do_ids': '|'.join(xref for xref in term['xrefs'] if xref.startswith('DOID:')),
                'hpo_ids': '|'.join(xref for xref in term['xrefs'] if xref.startswith('HP:')),
                'omim_ids': '|'.join(xref for xref in term['xrefs'] if xref.startswith('OMIM:')),
                'mesh_ids': '|'.join(xref for xref in term['xrefs'] if xref.startswith('MESH:'))

            }
