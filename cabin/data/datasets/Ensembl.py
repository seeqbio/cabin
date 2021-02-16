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
    version = '1'
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
    version = '1'
    depends = [EnsemblAnalysisFile]
    ensembl_table = 'analysis'
    tags = ['active']


# =====  exon_transcript   =====
class EnsemblExon_transcriptOfficial(EnsemblExternalFile):
    ensembl_table = 'exon_transcript'
    tags = ['active']


class EnsemblExon_transcriptFile(LocalFile):
    version = '1'
    depends = [EnsemblExon_transcriptOfficial]
    extension = 'txt.gz'


class EnsemblExon_transcriptTable(EnsemblTable):
    version = '1'
    depends = [EnsemblExon_transcriptFile]
    ensembl_table = 'exon_transcript'
    tags = ['active']


# ======   transcript   ======
class ensembl_transcriptOfficial(EnsemblExternalFile):
    ensembl_table = 'transcript'
    tags = ['active']


class ensembl_transcriptFile(LocalFile):
    version = '1'
    depends = [ensembl_transcriptOfficial]
    extension = 'txt.gz'


class ensembl_transcriptTable(EnsemblTable):
    version = '1'
    depends = [ensembl_transcriptFile]
    ensembl_table = 'transcript'
    tags = ['active']


# ======   translation   ======
class ensembl_translationOfficial(EnsemblExternalFile):
    ensembl_table = 'translation'
    tags = ['active']


class ensembl_translationFile(LocalFile):
    version = '1'
    depends = [ensembl_translationOfficial]
    extension = 'txt.gz'


class ensembl_translationTable(EnsemblTable):
    version = '1'
    depends = [ensembl_translationFile]
    ensembl_table = 'translation'
    tags = ['active']


# =====  protein_feauture   ======
class ensembl_protein_featureOfficial(EnsemblExternalFile):
    ensembl_table = 'protein_feature'
    tags = ['active']


class ensembl_protein_featureFile(LocalFile):
    version = '1'
    depends = [ensembl_protein_featureOfficial]
    extension = 'txt.gz'


class ensembl_protein_featureTable(EnsemblTable):
    version = '1'
    depends = [ensembl_protein_featureFile]
    ensembl_table = 'protein_feature'
    tags = ['active']


# =====  transcripti_attrib   =====
class ensembl_transcript_attribOfficial(EnsemblExternalFile):
    ensembl_table = 'transcript_attrib'
    tags = ['active']


class ensembl_transcript_attribFile(LocalFile):
    version = '1'
    depends = [ensembl_transcript_attribOfficial]
    extension = 'txt.gz'


class ensembl_transcript_attribTable(EnsemblTable):
    version = '1'
    depends = [ensembl_transcript_attribFile]
    ensembl_table = 'transcript_attrib'
    tags = ['active']


# ======    gene_attrib    ======
class ensembl_gene_attribOfficial(EnsemblExternalFile):
    ensembl_table = 'gene_attrib'
    tags = ['active']


class ensembl_gene_attribFile(LocalFile):
    version = '1'
    depends = [ensembl_gene_attribOfficial]
    extension = 'txt.gz'


class ensembl_gene_attribTable(EnsemblTable):
    version = '1'
    depends = [ensembl_gene_attribFile]
    ensembl_table = 'gene_attrib'
    tags = ['active']


# ======   attrib_type   ======
class EnsemblAttrib_typeOfficial(EnsemblExternalFile):
    ensembl_table = 'attrib_type'
    tags = ['active']


class EnsemblAttrib_typeFile(LocalFile):
    version = '1'
    depends = [EnsemblAttrib_typeOfficial]
    extension = 'txt.gz'


class EnsemblAttrib_typeTable(EnsemblTable):
    version = '1'
    depends = [EnsemblAttrib_typeFile]
    ensembl_table = 'attrib_type'
    tags = ['active']


# ======   seq_region    ======
class EnsemblSeq_regionOfficial(EnsemblExternalFile):
    ensembl_table = 'seq_region'
    tags = ['active']


class EnsemblSeq_regionFile(LocalFile):
    version = '1'
    depends = [EnsemblSeq_regionOfficial]
    extension = 'txt.gz'


class EnsemblSeq_regionTable(EnsemblTable):
    version = '1'
    depends = [EnsemblSeq_regionFile]
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
    version = '1'
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
    version = '1'
    depends = [EnsemblCdnaFile]

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
