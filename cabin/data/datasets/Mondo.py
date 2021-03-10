import networkx as nx

from biodb.mysql import MYSQL
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
    tags = ['active']

    columns = ['name', 'id', 'children', 'parents', 'hpo_ids', 'do_ids', 'omim_ids', 'mesh_ids']

    @property
    def schema(self):
        return """
          CREATE TABLE `{table}` (
            name       VARCHAR(255)   NOT NULL,         -- human readable disease name
            id         VARCHAR(255)   PRIMARY KEY,      -- unique identifier, eg: MONDO:0004355
            parents    VARCHAR(255)   NOT NULL,         -- pipe separated immediate parent
            children   TEXT           NOT NULL,         -- pipe separated immediate children
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

class MondoAncestryTable(RecordByRecordImportedTable):
    """Calculates all ancestor-descendant relationships in mondo by traversing
    the is-a DAG, as per the "parents" column of Mondo.

    Motivating use case: queries against various sources (e.g. ClinVar) where
    the user-specified disease id is allowed to be a descendant of diseases of
    interest.

    Example: give me variants associated with Leukemia, also include variants
    associated with subtypes like AML."""
    version = '3'
    depends = [MondoTable]
    tags = ['active']

    columns = ['descendant_id', 'ancestor_id']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                descendant_id  VARCHAR(255) NOT NULL,
                ancestor_id    VARCHAR(255) NOT NULL,
                INDEX (descendant_id),
                INDEX (ancestor_id)
            );
        """

    def read(self):
        dag = nx.DiGraph()

        with MYSQL.cursor() as cursor:
            cursor.execute('SELECT id, parents FROM `{mondo}`'.format(mondo=self.input.table_name))
            for disease_id, parents in cursor:
                dag.add_node(disease_id)
                if parents:
                    dag.add_edges_from([(parent, disease_id) for parent in parents.split('|')])

        # Naive traversal logic using nx.ancestors on every node. This means
        # traversing each edge many times over which takes time, can be
        # improved at the expense of writing a more involved traversal algo and
        # more memory usage.
        # This also doesn't have the ability to easily calculate the
        # edge-distance between each pair of nodes, no use case for this yet.
        for node in dag.nodes:
            yield {'descendant_id': node, 'ancestor_id': node}
            for ancestor in nx.ancestors(dag, node):
                yield {'descendant_id': node, 'ancestor_id': ancestor}

    def check(self):
        # every mondo entry should have one and only one row in the ancestry
        # table where the descendant and ancestor is itself, see read().
        with MYSQL.cursor() as cursor:
            cursor.execute('SELECT COUNT(*) FROM `{table}`'.format(table=self.input.table_name))
            mondo_count = cursor.fetchone()[0]

            cursor.execute('SELECT COUNT(*) FROM `{table}` WHERE descendant_id = ancestor_id'.format(table=self.table_name))
            matching_count = cursor.fetchone()[0]

            assert matching_count == mondo_count, \
                'Unexpected number of rows with matching descendant_id and ' \
                'ancestor_id. Expected %d, got %d' % (mondo_count, matching_count)
