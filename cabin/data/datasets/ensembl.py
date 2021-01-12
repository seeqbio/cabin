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
    version = '94'  # TODO: do we want to keep this here or in indiv. subclasses
    # depends definied in subclasses bc name specific

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
        pass

    def produce(self):
        # replcates produce of ImportedTable to create table based on schem specifications???????
        with NamedTemporaryFile('w') as temp:
            # NOTE if you don't unzip LOAD DATA does not complain and just puts
            # seemingly OK junk in the table!
            gunzip(self.input.path, temp.name)
            # LOAD DATA LOCAL INFILE needs allow_local_infile=True
            # https://dev.mysql.com/doc/connector-python/en/connector-python-connectargs.html
            with MYSQL.transaction(connection_kw={'allow_local_infile': True}) as cursor:
                cursor.create_table(self.table_name, self.schema)
                cursor.execute(r"""
                    LOAD DATA LOCAL INFILE '{path}'
                    INTO TABLE `{table}`
                """.format(path=temp.name, table=self.table_name))
                self._update_system_table(cursor)  # This is the only changed line from old ensembl base class


class ensembl_exonOfficial(EnsemblExternalFile):
    ensembl_table = 'exon'
    pass


class ensembl_exonFile(LocalFile):
    version = '1'
    depends = [ensembl_exonOfficial]
    extension = 'txt.gz'


class ensembl_exonTable(EnsemblTable):
    version = '1'
    depends = [ensembl_exonFile]
    ensembl_table = 'exon'


class ensembl_analysisOfficial(EnsemblExternalFile):
    ensembl_table = 'analysis'
    pass


class ensembl_analysisFile(LocalFile):
    version = '1'
    depends = [ensembl_analysisOfficial]
    extension = 'txt.gz'


class ensembl_analysisTable(EnsemblTable):
    version = '1'
    depends = [ensembl_analysisFile]
    ensembl_table = 'analysis'


class ensembl_exon_transcriptOfficial(EnsemblExternalFile):
    ensembl_table = 'exon_transcript'
    pass


class ensembl_exon_transcriptFile(LocalFile):
    version = '1'
    depends = [ensembl_exon_transcriptOfficial]
    extension = 'txt.gz'


class ensembl_exon_transcriptTable(EnsemblTable):
    version = '1'
    depends = [ensembl_exon_transcriptFile]
    ensembl_table = 'exon_transcript'


class ensembl_transcriptOfficial(EnsemblExternalFile):
    ensembl_table = 'transcript'
    pass


class ensembl_transcriptFile(LocalFile):
    version = '1'
    depends = [ensembl_transcriptOfficial]
    extension = 'txt.gz'


class ensembl_transcriptTable(EnsemblTable):
    version = '1'
    depends = [ensembl_transcriptFile]
    ensembl_table = 'transcript'


class ensembl_translationOfficial(EnsemblExternalFile):
    ensembl_table = 'translation'
    pass


class ensembl_translationFile(LocalFile):
    version = '1'
    depends = [ensembl_translationOfficial]
    extension = 'txt.gz'


class ensembl_translationTable(EnsemblTable):
    version = '1'
    depends = [ensembl_translationFile]
    ensembl_table = 'translation'


class ensembl_protein_featureOfficial(EnsemblExternalFile):
    ensembl_table = 'protein_feature'
    pass


class ensembl_protein_featureFile(LocalFile):
    version = '1'
    depends = [ensembl_protein_featureOfficial]
    extension = 'txt.gz'


class ensembl_protein_featureTable(EnsemblTable):
    version = '1'
    depends = [ensembl_protein_featureFile]
    ensembl_table = 'protein_feature'


class ensembl_transcript_attribOfficial(EnsemblExternalFile):
    ensembl_table = 'transcript_attrib'
    pass


class ensembl_transcript_attribFile(LocalFile):
    version = '1'
    depends = [ensembl_transcript_attribOfficial]
    extension = 'txt.gz'


class ensembl_transcript_attribTable(EnsemblTable):
    version = '1'
    depends = [ensembl_transcript_attribFile]
    ensembl_table = 'transcript_attrib'


class ensembl_gene_attribOfficial(EnsemblExternalFile):
    ensembl_table = 'gene_attrib'
    pass


class ensembl_gene_attribFile(LocalFile):
    version = '1'
    depends = [ensembl_gene_attribOfficial]
    extension = 'txt.gz'


class ensembl_gene_attribTable(EnsemblTable):
    version = '1'
    depends = [ensembl_gene_attribFile]
    ensembl_table = 'gene_attrib'


class ensembl_attrib_typeOfficial(EnsemblExternalFile):
    ensembl_table = 'attrib_type'
    pass


class ensembl_attrib_typeFile(LocalFile):
    version = '1'
    depends = [ensembl_attrib_typeOfficial]
    extension = 'txt.gz'


class ensembl_attrib_typeTable(EnsemblTable):
    version = '1'
    depends = [ensembl_attrib_typeFile]
    ensembl_table = 'attrib_type'


class ensembl_seq_regionOfficial(EnsemblExternalFile):
    ensembl_table = 'seq_region'
    pass


class ensembl_seq_regionFile(LocalFile):
    version = '1'
    depends = [ensembl_seq_regionOfficial]
    extension = 'txt.gz'


class ensembl_seq_regionTable(EnsemblTable):
    version = '1'
    depends = [ensembl_seq_regionFile]
    ensembl_table = 'seq_region'


class ensembl_geneOfficial(EnsemblExternalFile):
    ensembl_table = 'gene'
    pass


class ensembl_geneFile(LocalFile):
    version = '1'
    depends = [ensembl_geneOfficial]
    extension = 'txt.gz'


class ensembl_geneTable(EnsemblTable):
    version = '1'
    depends = [ensembl_geneFile]
    ensembl_table = 'gene'


class ensembl_cdnaOfficial(ExternalFile):
    version = '94'

    @property
    def url(self):
        return 'ftp://ftp.ensembl.org/pub/release-{v}/fasta/homo_sapiens/cdna/Homo_sapiens.GRCh38.cdna.all.fa.gz'.format(v=self.version)


class ensembl_cdnaFile(LocalFile):
    version = '1'
    depends = [ensembl_cdnaOfficial]
    extension = 'gz'


class ensembl_cdnaTable(RecordByRecordImportedTable):
    version = '1'
    depends = [ensembl_cdnaFile]

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
