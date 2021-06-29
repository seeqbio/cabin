import gzip
import sqlparse
from pathlib import Path
from tempfile import NamedTemporaryFile

from biodb import BiodbError
from biodb import AbstractAttribute
from biodb.io import logger
from biodb.io import wget
from biodb.io import gunzip
from biodb.io import read_fasta

from biodb.data.db import RecordByRecordImportedTable, ImportedTable
from biodb.data.files import LocalFile, ExternalFile
from biodb.mysql import MYSQL


def ensembl_schema(version):
    fname = 'homo_sapiens_core_{version}_38.sql.gz'.format(version=version)
    path = Path('/tmp/' + fname)
    dirname = 'pub/release-{version}/mysql/homo_sapiens_core_{version}_38'.format(version=version)
    ftp_url = 'ftp://ftp.ensembl.org/{dir}/{fname}'.format(dir=dirname, fname=fname)

    if not path.exists():
        logger.info('Downloading master SQL schema for Ensembl "{v}"'.format(v=version))
        retcode = wget(ftp_url, path)
        if retcode:
            path.unlink()
            raise BiodbError('Download failed!')

    logger.info('Loading master SQL schema for Ensembl "{v}" from: {p}'.format(v=version, p=path))
    with gzip.open(str(path), 'rt') as f:
        return f.read()


class EnsemblExternalFile(ExternalFile):
    version = '94'

    @property
    def url(self):
        fname = '{name}.txt.gz'.format(name=self.ensembl_table)
        dirname = 'pub/release-{version}/mysql/homo_sapiens_core_{version}_38'.format(version=self.version)
        return 'ftp://ftp.ensembl.org/{dir}/{fname}'.format(dir=dirname, fname=fname)


class EnsemblTable(ImportedTable):
    ensembl_table = AbstractAttribute()

    @property
    def schema(self):
        # ensembl schemas are dynamically built on the fly by parsing the
        # master ensembl schema. Difference from other dataset schemas.
        schema_sql = ensembl_schema(self.depends[0].depends[0].version)
        for sql in sqlparse.split(schema_sql):
            statement, = sqlparse.parse(sql)
            if statement.get_type() == 'CREATE':
                table_pos = [idx for idx, token in enumerate(statement.tokens) if token.value == 'TABLE']
                assert len(table_pos) == 1, 'unexpected CREATE statement: ' + statement.value

                table_pos = table_pos[0]

                # the fist Identifier following `TABLE` is the table name.
                for idx in range(table_pos, len(statement.tokens)):
                    if isinstance(statement.tokens[idx], sqlparse.sql.Identifier):
                        this_table = statement.tokens[idx].value
                        this_table_idx = idx
                        break
                else:
                    raise RuntimeError('CREATE TABLE statement does not have a table name:\n-> ' + statement.value)

                if this_table.replace('`', '') != self.ensembl_table:
                    continue

                logger.info('Found table "{t}", extracting table schema'.format(t=self.ensembl_table))

                # replace table name with `{table}` so it can be used by biodb
                new_tokens = statement.tokens.copy()
                new_tokens[this_table_idx:this_table_idx + 1] = sqlparse.parse('`%s`' % self.table_name)
                new_tokens[0:0] = sqlparse.parse('-- schema for ensembl.%s\n' % self.ensembl_table)
                return sqlparse.sql.Statement(new_tokens).value
        else:
            raise BiodbError('Failed to find table "%s" in Ensembl schema!' % self.ensembl_table)

    def import_table(self, cursor):
        with NamedTemporaryFile('w') as temp:
            # NOTE if you don't unzip LOAD DATA does not complain and just puts
            # seemingly OK junk in the table!
            gunzip(self.input.path, temp.name)
            # LOAD DATA LOCAL INFILE needs allow_local_infile=True
            # https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
            with MYSQL.transaction(connection_kw={'allow_local_infile': True}) as cursor:
                cursor.execute(r"""
                    LOAD DATA LOCAL INFILE '{path}'
                    INTO TABLE `{table}`
                """.format(path=temp.name, table=self.table_name))


# ========    exon    ========
class EnsemblExonOfficial(EnsemblExternalFile):
    ensembl_table = 'exon'


class EnsemblExonFile(LocalFile):
    version = '1'
    depends = [EnsemblExonOfficial]
    extension = 'txt.gz'


class EnsemblExonTable(EnsemblTable):
    version = '2'
    depends = [EnsemblExonFile]
    ensembl_table = 'exon'
    tags = ['active']


# =======   analysis     =======
class EnsemblAnalysisOfficial(EnsemblExternalFile):
    ensembl_table = 'analysis'
    tags = ['active']


class EnsemblAnalysisFile(LocalFile):
    version = '1'
    depends = [EnsemblAnalysisOfficial]
    extension = 'txt.gz'


class EnsemblAnalysisTable(EnsemblTable):
    version = '2'
    depends = [EnsemblAnalysisFile]
    ensembl_table = 'analysis'
    tags = ['active']


# =====  exon_transcript   =====
class EnsemblExonTranscriptOfficial(EnsemblExternalFile):
    ensembl_table = 'exon_transcript'
    tags = ['active']


class EnsemblExonTranscriptFile(LocalFile):
    version = '1'
    depends = [EnsemblExonTranscriptOfficial]
    extension = 'txt.gz'


class EnsemblExonTranscriptTable(EnsemblTable):
    version = '2'
    depends = [EnsemblExonTranscriptFile]
    ensembl_table = 'exon_transcript'
    tags = ['active']


# ======   transcript   ======
class EnsemblTranscriptOfficial(EnsemblExternalFile):
    ensembl_table = 'transcript'
    tags = ['active']


class EnsemblTranscriptFile(LocalFile):
    version = '1'
    depends = [EnsemblTranscriptOfficial]
    extension = 'txt.gz'


class EnsemblTranscriptTable(EnsemblTable):
    version = '2'
    depends = [EnsemblTranscriptFile]
    ensembl_table = 'transcript'
    tags = ['active']


# ======   translation   ======
class EnsemblTranslationOfficial(EnsemblExternalFile):
    ensembl_table = 'translation'
    tags = ['active']


class EnsemblTranslationFile(LocalFile):
    version = '1'
    depends = [EnsemblTranslationOfficial]
    extension = 'txt.gz'


class EnsemblTranslationTable(EnsemblTable):
    version = '2'
    depends = [EnsemblTranslationFile]
    ensembl_table = 'translation'
    tags = ['active']


# =====  protein_feauture   ======
class EnsemblProteinFeatureOfficial(EnsemblExternalFile):
    ensembl_table = 'protein_feature'
    tags = ['active']


class EnsemblProteinFeatureFile(LocalFile):
    version = '1'
    depends = [EnsemblProteinFeatureOfficial]
    extension = 'txt.gz'


class EnsemblProteinFeatureTable(EnsemblTable):
    version = '2'
    depends = [EnsemblProteinFeatureFile]
    ensembl_table = 'protein_feature'
    tags = ['active']


# =====  transcripti_attrib   =====
class EnsemblTranscriptAttribOfficial(EnsemblExternalFile):
    ensembl_table = 'transcript_attrib'
    tags = ['active']


class EnsemblTranscriptAttribFile(LocalFile):
    version = '1'
    depends = [EnsemblTranscriptAttribOfficial]
    extension = 'txt.gz'


class EnsemblTranscriptAttribTable(EnsemblTable):
    version = '2'
    depends = [EnsemblTranscriptAttribFile]
    ensembl_table = 'transcript_attrib'
    tags = ['active']


# ======    gene_attrib    ======
class EnsemblGeneAttribOfficial(EnsemblExternalFile):
    ensembl_table = 'gene_attrib'
    tags = ['active']


class EnsemblGeneAttribFile(LocalFile):
    version = '1'
    depends = [EnsemblGeneAttribOfficial]
    extension = 'txt.gz'


class EnsemblGeneAttribTable(EnsemblTable):
    version = '2'
    depends = [EnsemblGeneAttribFile]
    ensembl_table = 'gene_attrib'
    tags = ['active']


# ======   attrib_type   ======
class EnsemblAttribTypeOfficial(EnsemblExternalFile):
    ensembl_table = 'attrib_type'
    tags = ['active']


class EnsemblAttribTypeFile(LocalFile):
    version = '1'
    depends = [EnsemblAttribTypeOfficial]
    extension = 'txt.gz'


class EnsemblAttribTypeTable(EnsemblTable):
    version = '2'
    depends = [EnsemblAttribTypeFile]
    ensembl_table = 'attrib_type'
    tags = ['active']


# ======   seq_region    ======
class EnsemblSeqRegionOfficial(EnsemblExternalFile):
    ensembl_table = 'seq_region'
    tags = ['active']


class EnsemblSeqRegionFile(LocalFile):
    version = '1'
    depends = [EnsemblSeqRegionOfficial]
    extension = 'txt.gz'


class EnsemblSeqRegionTable(EnsemblTable):
    version = '2'
    depends = [EnsemblSeqRegionFile]
    ensembl_table = 'seq_region'
    tags = ['active']


# ======     gene     ======
class EnsemblGeneOfficial(EnsemblExternalFile):
    ensembl_table = 'gene'
    tags = ['active']


class EnsemblGeneFile(LocalFile):
    version = '1'
    depends = [EnsemblGeneOfficial]
    extension = 'txt.gz'


class EnsemblGeneTable(EnsemblTable):
    version = '2'
    depends = [EnsemblGeneFile]
    ensembl_table = 'gene'
    tags = ['active']


# ======     cdna     ======
class EnsemblCdnaOfficial(ExternalFile):
    version = '94'

    @property
    def url(self):
        return 'ftp://ftp.ensembl.org/pub/release-{v}/fasta/homo_sapiens/cdna/Homo_sapiens.GRCh38.cdna.all.fa.gz'.format(v=self.version)


class EnsemblCdnaFile(LocalFile):
    version = '1'
    depends = [EnsemblCdnaOfficial]
    extension = 'gz'


class EnsemblCdnaTable(RecordByRecordImportedTable):
    version = '2'
    depends = [EnsemblCdnaFile]
    tags = ['active']
    columns = ['ensembl_transcript', 'cdna']

    @property
    def schema(self):
        return """
            CREATE TABLE `{table}` (
                ensembl_transcript VARCHAR(255) NOT NULL UNIQUE,
                cdna               LONGTEXT NOT NULL
            );
        """

    def read(self):
        for name, sequence in read_fasta(self.input.path, gzipped=True):
            # drop the ".X" version
            yield {'ensembl_transcript': name.split('.')[0],
                   'cdna': sequence}


# ======     Derived exon & transcript info     ======
from biodb.mysql import MYSQL
from biodb.data.db import RecordByRecordImportedTable


class SGXCodingExonTable(RecordByRecordImportedTable):
    """ Coding exon information per coding transcript, such that exon-transcript
    pair are unique. Info captures the coding region (via the translation table)
        hypothetical ex:
            exon rank 3 --> cds starts at position 12 (inclusive)
            exons 4, 5, 6 --> internal are coding, cds for full exon length
            exon rank 7 --> cds ends at position 58 (exclusive)
        Note that exons 1,2 are not represented (not coding)
    """


    version = '1'
    depends = [
        EnsemblExonTranscriptTable,
        EnsemblTranscriptTable,
        EnsemblGeneTable,
        EnsemblExonTable,
        EnsemblTranslationTable
    ]

    @property
    def depends_table_names(cls):
       return {ds().type: ds().name for ds in cls.depends}
 
    tags = ['active']

    columns = [
        'ensg',
        'enst',
        'ense',
        'exon_rank',
        'coding',
        'coding_start',
        'coding_end'
    ]

    @property
    def schema(self):
        return """
          CREATE TABLE `{table}` (
            ensg                 VARCHAR(255)   NOT NULL,         -- ensemble stable gene id, eg: ENSG00000138413
            enst                 VARCHAR(255)   NOT NULL,         -- ensembl stable transcript id, eg: ENST00000345146
            ense                 VARCHAR(255)   NOT NULL,         -- ensembl stable exon ide, eg: ENSE00003564564
            exon_rank            VARCHAR(255)   NOT NULL,         -- order of exons (1-based), only includes cds exons. eg: 3
            coding               VARCHAR(255)   NOT NULL,         -- True if the exon is coding, False otherwise
            coding_start         VARCHAR(255)   NULL,             -- sgx specified inclusive, Null if not coding
            coding_end           VARCHAR(255)   NULL,             -- sgx specified exclusing, such that length = coding_end - coding_start
            INDEX (ensg)
          );
        """

    def read(self):
        """ For each transcript, run `enst_exons()` to select all exons, combine
        using arithmetic to exon-by-transcript properties. Note: the resulting 
        row is primarily generate from enst_exon, hence adding repeating info
        of transcripts (enst, ensg) for each row.
        """
        def enst_exons(enst):
            with MYSQL.cursor(dictionary=True) as cursor:
                query = """
                    SELECT e.exon_id, e.stable_id as ense, et.rank as exon_rank, e.seq_region_start, e.seq_region_end
                    FROM `{EnsemblExonTranscriptTable}` et
                    JOIN `{EnsemblTranscriptTable}` t using (transcript_id)
                    JOIN `{EnsemblExonTable}` e USING (exon_id)
                    WHERE t.stable_id = %s;
                """.format(**self.depends_table_names)
                cursor.execute(query, (enst,))
                exons = cursor.fetchall()
                return exons

        with MYSQL.cursor(dictionary=True) as cursor:
            query = """
                SELECT t.stable_id as transcript_stable_id, g.stable_id as gene_id,
                       tl.start_exon_id, tl.end_exon_id, tl.seq_start, tl.seq_end
                FROM `{EnsemblTranscriptTable}` t
                JOIN `{EnsemblTranslationTable}` tl using (transcript_id)
                JOIN `{EnsemblGeneTable}` g using(gene_id);
            """.format(**self.depends_table_names)
            cursor.execute(query)
            transcripts = cursor.fetchall()

        for transcript in transcripts:
            exon_by_id = {exon['exon_id']: exon for exon in enst_exons(transcript['transcript_stable_id'])}
            start_rank = exon_by_id[transcript['start_exon_id']]['exon_rank']
            end_rank = exon_by_id[transcript['end_exon_id']]['exon_rank']

            for exon in exon_by_id.values():
                # seq_region_start and seq_region_end are inclusive (ensemble-specified)
                # coding_start is inclusive, coding_end is exclusive (sgx-specified), eg: 
                exon_length = abs(exon['seq_region_start'] - exon['seq_region_end']) + 1
                if start_rank <= exon['exon_rank'] <= end_rank:
                    exon['coding'] = True
                    exon['coding_start'] = transcript['seq_start'] if exon['exon_rank'] == start_rank else 1
                    exon['coding_end'] = transcript['seq_end'] + 1 if exon['exon_rank'] == end_rank else exon_length + 1 
                else:
                    # exon is entirely in either of 5' or 3' UTRs
                    exon['coding'] = False
                    exon['coding_start'] = exon['coding_end'] = None
                exon['enst'] = transcript['transcript_stable_id'] 
                exon['ensg'] = transcript['gene_id'] 
                yield exon


class SGXTranscriptInfoTable(ImportedTable):
    """ This table is just the join of various tables - do we really need it?

    The purpose of this table is to collect the exon information that will 
    be used for the transcript info table, which will be used for the
    representative transcript.
    """


    version = '1'
    from biodb.data.datasets.Gene2Ensembl import Gene2EnsemblTable
    depends = [SGXCodingExonTable, Gene2EnsemblTable]

    @property
    def depends_table_names(cls):
        return {ds().type: ds().name for ds in cls.depends}

    tags = ['active']

    @property
    def schema(self):
        return """
          CREATE TABLE `{table}` (
            ensg                     VARCHAR(255)   NOT NULL,       -- ensemble stable exon id, eg: ENSG00000138413
            enst                     VARCHAR(255)   NOT NULL,       -- ensembl stable transcript id, eg: ENST00000345146
            refseq_transcript        VARCHAR(255)   NULL,           -- matching nm if exists, eg: NM_005896
            cds_length               INTEGER        NOT NULL        -- sum of protein-coding nucletides
          );
        """

    def import_table(self, cursor):
        sql_insert = """
            INSERT INTO `{table}`(ensg, enst, refseq_transcript, cds_length)
            SELECT se.ensg, se.enst, g.refseq_transcript, SUM(se.coding_end - se.coding_start) as cds_length
            FROM `{SGXCodingExonTable}` as se
            JOIN `{Gene2EnsemblTable}` g ON se.enst=g.ensembl_transcript
            GROUP BY enst, ensg;
        """.format(table=self.table_name, **self.depends_table_names)
        cursor.execute(sql_insert)
