from biodb.io import read_xsv
from biodb.data.db import ImportedTable, RecordByRecordImportedTable
from biodb.data.files import (
    FTPTimestampedFile,
    S3MirrorFile,
    S3MirroredLocalFile
)

MAX_VARIANT_LENGTH = 250


class ClinVarH4VOfficial(FTPTimestampedFile):
    version = '2021-02-13'  # Typically updates with same frequence as VCF, check ftp date
    ftp_server = 'ftp.ncbi.nih.gov'
    ftp_path = '/pub/clinvar/tab_delimited/hgvs4variation.txt.gz'


class ClinVarH4VS3Mirror(S3MirrorFile):
    version = '1'
    depends = [ClinVarH4VOfficial]
    extension = 'txt.gz'


class ClinVarH4VFile(S3MirroredLocalFile):
    version = '1'
    depends = [ClinVarH4VS3Mirror]
    extension = 'gz'


class ClinVarH4VTable(RecordByRecordImportedTable):
    version = '2'
    depends = [ClinVarH4VFile]
    tags = ['active']

    columns = [
        'gene_symbol',
        'gene_id',
        'variation_id',
        'allele_id',
        'type',
        'refseq_transcript',
        'hgvs_c',
        'refseq_protein',
        'hgvs_p']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
              gene_symbol            VARCHAR(255) NOT NULL,
              gene_id                VARCHAR(255) NOT NULL,        -- NCBI gene identifier (integer stored as string)
              variation_id           VARCHAR(255) NOT NULL,
              allele_id              VARCHAR(255) NOT NULL,
              type                   VARCHAR(255) NOT NULL,        -- nucleotide type (coding, genomic, etc)
              refseq_transcript      VARCHAR(255) NOT NULL,        -- substring of NucleotideExpression containing the refsequ identifier (NP, XM, etc)
              hgvs_c                 VARCHAR(255) NOT NULL,
              refseq_protein         VARCHAR(255) NULL,            -- substring of ProteinExpression containing the refseq identifier (NP, XP, etc)
              hgvs_p                 VARCHAR(255) NULL,
              INDEX (refseq_transcript),
              INDEX (variation_id),
              INDEX (gene_symbol),
              INDEX (gene_id)
            );
    """

    def read(self):
        hgvs_fields = [
            'Symbol',
            'GeneID',
            'VariationID',
            'AlleleID',
            'Type',
            'Assembly',
            'NucleotideExpression',
            'NucleotideChange',
            'ProteinExpression',
            'ProteinChange',
            'UsedForNaming',
            'Submitted',
            'OnRefSeqGene']

        for row in read_xsv(self.input.path, header_leading_hash=False, ignore_leading_hash=True, columns=hgvs_fields, gzipped=True):
            if row['Type'] == 'genomic':
                continue
            if row['NucleotideChange'] == '-':
                continue  # Drop rows with no hgvs_c, lacks meaning

            if row['NucleotideExpression'] != '-':
                refseq_transcript = row['NucleotideExpression'].split(':')[0]
            else:
                continue  # Drop rows that are LRG* accessions and redundant with type=genomic

            if row['ProteinExpression'] != '-':
                refseq_protein = row['ProteinExpression'].split(':')[0]
            else:
                refseq_protein = None
                row['ProteinChange'] = None  # unlike hgvs_c, hgvs_p may be NULL if non-coding

            if row['ProteinChange'] and len(row['ProteinChange']) > MAX_VARIANT_LENGTH:
                continue  # Drop rows with complex structural changes
            if row['NucleotideChange'] and len(row['NucleotideChange']) > MAX_VARIANT_LENGTH:
                continue  # Drop rows with complex structural changes

            rec = {
                'gene_symbol': row['Symbol'],
                'gene_id': row['GeneID'],
                'variation_id': row['VariationID'],
                'allele_id': row['AlleleID'],
                'type': row['Type'],
                'refseq_transcript': refseq_transcript,
                'hgvs_c': row['NucleotideChange'],
                'refseq_protein': refseq_protein,
                'hgvs_p': row['ProteinChange']
            }
            yield rec


class ClinVarVariationToGeneTable(ImportedTable):
    version = '1'
    depends = [ClinVarH4VTable]
    tags = ['active']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                variation_id      VARCHAR(255)  NOT NULL,
                gene_id           VARCHAR(255) NOT NULL,
                gene_symbol       VARCHAR(255) NOT NULL,
                INDEX(variation_id),
                INDEX(gene_id),
                INDEX(gene_symbol)
            )
        """

    def import_table(self, cursor):
        sql_insert = """
                INSERT INTO `{table}`(variation_id, gene_symbol, gene_id)
                SELECT variation_id, gene_symbol, gene_id
                FROM `{H4V}`
                GROUP BY variation_id, gene_symbol, gene_id;
            """.format(table=self.table_name, H4V=self.input.table_name)
        cursor.execute(sql_insert)
