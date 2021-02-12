from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_xsv


class CancerMineCollatedOfficial(ExternalFile):
    version = '3525385'

    @property
    def url(self):
        return 'https://zenodo.org/record/{v}/files/cancermine_collated.tsv?download=1'.format(v=self.version)

class CancerMineCollatedFile(LocalFile):
    version = '1'
    depends = [CancerMineCollatedOfficial]
    extension = 'tsv'


class CancerMineCollatedTable(RecordByRecordImportedTable):
    version = '1'
    depends = [CancerMineCollatedFile]

    columns = ['matching_id', 'role', 'do_id', 'cancer_normalized', 'gene_entrez_id', 'citation_count']

    @property
    def schema(self):
        return """
        CREATE TABLE `{table}` (
            matching_id               VARCHAR(225) NOT NULL,
            role                      VARCHAR(225) NOT NULL,
            do_id                     VARCHAR(255) NOT NULL,    -- Disease Ontology id, eg: DOID:10747
            cancer_normalized         VARCHAR(225) NOT NULL,    -- term used in ontologoes
            gene_entrez_id            VARCHAR(225) NOT NULL,
            citation_count            VARCHAR(225) NOT NULL,
            INDEX (matching_id),
            INDEX (gene_entrez_id)
        );
    """

    def read(self):
        for row in read_xsv(self.input.path, header_leading_hash=False):
            row['do_id'] = row['cancer_id']
            yield row


class CancerMineSentencesOfficial(ExternalFile):
    version = '3525385'

    @property
    def url(self):
        return 'https://zenodo.org/record/{v}/files/cancermine_sentences.tsv?download=1'.format(v=self.version)

class CancerMineSentencesFile(LocalFile):
    version = '1'
    depends = [CancerMineSentencesOfficial]
    extension = 'tsv'


class CancerMineSentencesTable(RecordByRecordImportedTable):
    # sentence                  TEXT NOT NULL,    -- sentence in literature
    # FAILING for myswl '\xCE\xB2-cat...' for column 'sentence' at row 1 error

    version = '1'
    depends = [CancerMineSentencesFile]

    columns = ['matching_id', 'pmid', 'predictprob', 'gene_entrez_id'] # FIXME: after unicode fix, add back , 'sentence']

    @property
    def schema(self):
        return """
    CREATE TABLE `{table}` (
    matching_id               VARCHAR(225) NOT NULL,
    pmid                      VARCHAR(225) NOT NULL,
    predictprob               VARCHAR(225) NOT NULL,
    gene_entrez_id            VARCHAR(225) NOT NULL,
    INDEX (matching_id),
    INDEX (gene_entrez_id)
);
    """

    def read(self):
        for row in read_xsv(self.input.path, header_leading_hash=False, encoding='utf-8'):
            yield row
