#from biodb.io import read_xsv
#from biodb.base import BaseDataset
#
#from biodb.mixins import RecordByRecordImportMixin
#
from biodb.data.db import RecordByRecordImportMixin
from biodb.data.db import ImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.io import read_xsv


class CancerMineCollatedOfficial(ExternalFile):
    version = '3525385'

    @property
    def url(self):
        return 'https://zenodo.org/record/{v}/files/cancermine_collated.tsv?download=1'.format(v=self.version)

class CancerMineCollatedFile(LocalFile):
    version = '1'
    depends = {'CancerMineCollatedOfficial': CancerMineCollatedOfficial}
    extension = 'tsv'


class CancerMineCollatedTable(RecordByRecordImportMixin, ImportedTable):
    version = '1'
    depends = {'CancerMineCollatedFile': CancerMineCollatedFile}

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
        for row in read_xsv(self.inputs['CancerMineCollatedFile'].path, header_leading_hash=False):
            row['do_id'] = row['cancer_id']
            yield row
#
#class CancerMineCollated(RecordByRecordImportMixin, BaseDataset):
#    name = 'CancerMine.Collated'
#    file_extension = 'tsv'
#    columns = ['matching_id', 'role', 'do_id', 'cancer_normalized', 'gene_entrez_id', 'citation_count']
#
#    @property
#    def source_url(self):
#        return 'https://zenodo.org/record/{v}/files/cancermine_collated.tsv?download=1'.format(v=self.version.source)
#
#    def read(self):
#        for row in read_xsv(self.download_path, header_leading_hash=False):
#            row['do_id'] = row['cancer_id']  # rename to match other xrefs
#            yield row
#
#
#class CancerMineSentences(RecordByRecordImportMixin, BaseDataset):
#    name = 'CancerMine.Sentences'
#    file_extension = 'tsv'
#
#    columns = ['matching_id', 'pmid', 'predictprob', 'gene_entrez_id', 'sentence']
#
#    @property
#    def source_url(self):
#        return 'https://zenodo.org/record/{v}/files/cancermine_sentences.tsv?download=1'.format(v=self.version.source)
#
#    def read(self):
#        for row in read_xsv(self.download_path, header_leading_hash=False):
#            yield row
