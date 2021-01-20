from biodb.io import read_xsv
from biodb.data.db import RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)


class Gene2EnsemblOfficial(FTPTimestampedFile):
    version = '2021-01-18'
    ftp_server = 'ftp.ncbi.nih.gov'
    ftp_path = '/gene/DATA/gene2ensembl.gz'


class Gene2EnsemblS3Mirror(S3MirrorFile):
    version = '1'
    depends = [Gene2EnsemblOfficial]
    extension = 'gz'


class Gene2EnsemblFile(S3MirroredLocalFile):
    version = '1'
    depends = [Gene2EnsemblS3Mirror]
    extension = 'gz'


class Gene2EnsemblTable(RecordByRecordImportedTable):
    version = '1'
    depends = [Gene2EnsemblFile]
    columns = ['refseq_transcript', 'ensembl_transcript', 'gene_id', 'ensembl_gene_id']
    field_mappings = {
        'RNA_nucleotide_accession.version': 'refseq_transcript',
        'Ensembl_rna_identifier': 'ensembl_transcript',
        'GeneID': 'gene_id',
        'Ensembl_gene_identifier': 'ensembl_gene_id',
    }

    @property
    def schema(self):
        return """
            CREATE TABLE IF NOT EXISTS `{table}` (
                ensembl_transcript    VARCHAR(255) NOT NULL UNIQUE, -- Ensembl mRNA stable ID (ENST...)
                refseq_transcript     VARCHAR(255) NOT NULL UNIQUE, -- refseq mRNA accession ID (NM...)
                gene_id               VARCHAR(255) NOT NULL,        -- NCBI gene identifier (integer stored as string)
                ensembl_gene_id       VARCHAR(255) NOT NULL         -- Ensemble gene identifier (ENSG...)
            );
        """

    def read(self):
        for row in read_xsv(self.input.path, gzipped=True):
            if row['tax_id'] != '9606':
                continue
            if row['RNA_nucleotide_accession.version'] == '-':
                continue
            yield row

    def transform(self, record):
        record = super().transform(record)
        record['refseq_transcript'] = record['refseq_transcript'].split('.')[0]
        record['ensembl_transcript'] = record['ensembl_transcript'].split('.')[0]
        return record
