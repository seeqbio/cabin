import re

from biodb.io import read_xsv
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class Gene2RefSeqOfficial(FTPTimestampedFile):
    version = '2021-01-18'
    ftp_server = 'ftp.ncbi.nih.gov'
    ftp_path = '/gene/DATA/gene2refseq.gz'


class Gene2RefSeqS3Mirror(S3MirrorFile):
    version = '1'
    depends = [Gene2RefSeqOfficial]
    extension = 'gz'


class Gene2RefSeqFile(S3MirroredLocalFile):
    version = '1'
    depends = [Gene2RefSeqS3Mirror]
    extension = 'gz'


class Gene2RefSeqTable(RecordByRecordImportedTable):
    version = '1'
    depends = [Gene2RefSeqFile]
    tags = ['active']

    columns = [
        'refseq_transcript',
        'refseq_protein',
        'refseq_genomic',
        'gene_id',
        'gene_symbol'
    ]
    field_mappings = {
        'RNA_nucleotide_accession.version': 'refseq_transcript',
        'protein_accession.version': 'refseq_protein',
        'genomic_nucleotide_accession.version': 'refseq_genomic',
        'GeneID': 'gene_id',
        'Symbol': 'gene_symbol',
    }

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                refseq_transcript         VARCHAR(255) NOT NULL,    -- refseq mRNA accession ID
                refseq_protein            VARCHAR(255) NOT NULL,    -- refseq protein accession ID
                refseq_genomic            VARCHAR(255) NOT NULL,    -- refseq chromosome accession ID
                gene_id                   VARCHAR(255) NOT NULL,    -- NCBI gene identifier (integer stored as string)
                gene_symbol               VARCHAR(255) NOT NULL,    -- HGNC gene symbol
                INDEX (gene_symbol),
                INDEX (refseq_protein)
            );
        """

    def read(self):
        for row in read_xsv(self.input.path, gzipped=True):
            if row['tax_id'] != '9606':
                continue
            if not re.match('.*GRCh38.*Primary Assembly', row['assembly']):
                continue
            if row['RNA_nucleotide_accession.version'] == '-':
                continue
            yield row

    def transform(self, record):
        record = super().transform(record)
        # drop the ".X" version
        record['refseq_transcript'] = record['refseq_transcript'].split('.')[0]
        record['refseq_protein'] = record['refseq_protein'].split('.')[0]
        record['refseq_genomic'] = record['refseq_genomic'].split('.')[0]
        return record
