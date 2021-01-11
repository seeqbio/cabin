import gzip
import sqlparse
from pathlib import Path
from tempfile import NamedTemporaryFile

from biodb import BiodbError
from biodb import AbstractAttribute
from biodb.io import logger
from biodb.io import wget
from biodb.io import gunzip

from biodb.data.db import ImportedTable
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
