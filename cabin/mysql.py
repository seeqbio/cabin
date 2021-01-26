import os
import sys
import types
import getpass
import textwrap
from time import sleep
import mysql.connector
from contextlib import contextmanager

from biodb import logger
from biodb import settings
from biodb import BiodbError


READER = 'reader'
WRITER = 'writer'


class _MySQL:
    passwords = {READER: settings.SGX_MYSQL_READER_PASSWORD,
                 WRITER: settings.SGX_MYSQL_WRITER_PASSWORD}

    def __init__(self, profile=False):
        self.profile = profile

    @contextmanager
    def transaction(self, connection_kw={}, cursor_kw={}):

        def create_table(cursor, table_name, query):
            assert isinstance(table_name, str), 'bad table name: ' + table_name
            # MySQL has a maximum table name length of 64
            # https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
            if len(table_name) > 64:
                raise BiodbError('Table name `%s` exceeds the maximum allowed 64 characters' % table_name)
            cursor._created_tables.append(table_name)
            cursor.execute(query)

        def drop_created_tables(cursor):
            for table in cursor._created_tables:
                cursor.execute('DROP TABLE IF EXISTS `%s`;' % table)

        with self.connection('writer', **connection_kw) as cnx:
            cnx.start_transaction()
            with cnx.cursor(**cursor_kw) as cursor:
                cursor._created_tables = []
                # monkey patch a new method on the cursor we produce; we need a
                # separate method for table creation to allow clean rollbacks.
                # This is because CREATE/DROP table is not rollbackable by
                # MySQL but we really need to at least rollback CREATE's to
                # simplifying testing (failed import should fail cleanly and
                # not leave a partial or empty table around).
                cursor.create_table = types.MethodType(create_table, cursor)
                cursor.drop_created_tables = types.MethodType(drop_created_tables, cursor)
                try:
                    yield cursor
                    cnx.commit()
                # catch all exceptions, even KeyboardInterrupt (i.e. ctrl+C)
                # we must ensure to re-raise it.
                # NOTE it seems like cursor.execute swallows keyboard
                # interrupts!
                except:
                    cursor.drop_created_tables()
                    cnx.rollback()
                    raise

    def wait_for_connection(self, retry_every=1, timeout=30, **cnx_kw):
        """Returns a connection to MySQL with the given connection kwargs, but
        retries a number of times before raising an exception.

        Args:
            retry_every (int): # of seconds to wait between trials to connect.
            timout      (int): # of seconds after which aborts retrying.
        """
        last_exception = None
        time_left = timeout
        while time_left > 0:
            try:
                cnx = mysql.connector.connect(**cnx_kw)
                if last_exception is not None:
                    sys.stderr.write('\n')
                return cnx
            except Exception as e:
                if last_exception is None:
                    sys.stderr.write('Waiting for MySQL server to accept connections .')
                else:
                    sys.stderr.write('.')
                sys.stderr.flush()

                last_exception = e
                sleep(retry_every)
                time_left -= retry_every

        sys.stderr.write('\n%s\n' % str(last_exception))
        raise BiodbError('Failed to connect to MySQL after %d seconds!' % timeout)

    @contextmanager
    def connection(self, user, **kw):
        cnx_kw = {
            'user': user,
            'password': self.passwords[user],
            'host': settings.SGX_MYSQL_HOST,
            'database': settings.SGX_MYSQL_DB,
        }
        cnx_kw.update(**kw)
        cnx = self.wait_for_connection(**cnx_kw)
        real_cursor = cnx.cursor

        @contextmanager
        def cursor_wrapper(*args, **kwargs):

            cursor = real_cursor(cnx, *args, **kwargs)
            if self.profile:
                cursor.execute('SET profiling = 1')

            try:
                yield cursor
            finally:
                if self.profile:
                    cursor.execute('SHOW profiles;')
                    for row in cursor:
                        idx, time_s, query = row
                        time_ms = '{t} ms'.format(t=round(time_s * 1000, 2)).ljust(10)
                        query = '\n\t\t\t'.join(textwrap.wrap(query, 80))
                        logger.info('  {time}{query}'.format(idx=idx, time=time_ms, query=query))
                cursor.close()

        cnx.cursor = cursor_wrapper
        try:
            yield cnx
        finally:
            cnx.close()

    @contextmanager
    def cursor(self, user, **kwargs):
        with self.connection(user) as cnx:
            with cnx.cursor(**kwargs) as cursor:
                yield cursor


    def _get_root_connection(self):
        if 'SGX_MYSQL_ROOT_PASSWORD' in os.environ:
            root_password = os.environ['SGX_MYSQL_ROOT_PASSWORD']
        else:
            root_password = getpass.getpass("Enter root password (given to you): ")

        return self.wait_for_connection(
            user='root',
            host=settings.SGX_MYSQL_HOST,
            password=root_password
        )

    def seems_initialized(self):
        cnx = self._get_root_connection()
        cursor = cnx.cursor()
        cursor.execute('SELECT user FROM mysql.user WHERE user = "%s"' % READER)
        return bool(cursor.fetchone())

    def initialize(self):
        database = settings.SGX_MYSQL_DB

        if self.seems_initialized():
            raise BiodbError('Database `%s` seems to be already initialized!' % database)

        cnx = self._get_root_connection()

        cursor = cnx.cursor()
        cursor.execute('CREATE DATABASE IF NOT EXISTS {db};'.format(db=database))
        logger.info('created database "%s"' % database)

        def _create(user):
            # intentionally not using `CREATE USER IF NOT EXISTS` since that
            # would mislead the user into thinking they can reset the passwords
            # if they just re-initialized.
            cursor.execute("""
                CREATE USER '{user}'@'%'
                IDENTIFIED BY '{password}';
            """.format(user=user, password=self.passwords[user]))
            logger.info('created user "%s"' % user)

        def _grant(grant, user):
            cursor.execute("""
                GRANT {grant} ON `{db}`.* TO "{user}"@"%"
            """.format(grant=grant, db=database, user=user))
            logger.info('granted "%s" on "%s" to user "%s"' % (grant, database, user))

        def _add_system_table():
            logger.info('--> initializing database')
            query = ("""
                USE {db};
                CREATE TABLE IF NOT EXISTS `system` (
                    sha         VARCHAR(64)  PRIMARY KEY,
                    type        VARCHAR(128),
                    name        VARCHAR(255),
                    table_name  VARCHAR(255),
                    formula     VARCHAR(4096)
                );
            """).format(db=database)
            cursor.execute(query)


        _create(READER)
        _grant('SELECT', READER)

        _create(WRITER)
        _grant('ALL PRIVILEGES', WRITER)

        cursor.execute('FLUSH PRIVILEGES;')

        # Set global system variable to allow loading data into tables
        # from local files (see biodb/datasets/pfam.py).
        # Docs: https://dev.mysql.com/doc/refman/8.0/en/persisted-system-variables.html
        # Docs: https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_local_infile
        # NOTE this is a SQL syntax error in 5.7
        cursor.execute("SET PERSIST local_infile = 'ON';")

        _add_system_table()


        cursor.close()

        logger.info('successfully initialized "{db}"!'.format(db=database))

    def drop_users(self):
        cnx = self._get_root_connection()
        cursor = cnx.cursor()

        cursor.execute("DROP USER '{user}'@'%'".format(user=READER))
        logger.info('dropped user "%s"!' % READER)

        cursor.execute("DROP USER '{user}'@'%'".format(user=WRITER))
        logger.info('dropped user "%s"!' % WRITER)

        cursor.execute('FLUSH PRIVILEGES;')
        cursor.close()

    def shell(self, user):
        """Executes mysql client in _this_ process (replaces python process
        immediately). This is necessary; using a subprocess produces weird
        behavior with how interrupts are handled, e.g. how ctrl+c works.

        Note on `os.execvp` usage:
            The first argument to execvp is the executable name (searched for
            in $PATH) and the second argument is the argv list passed to the
            process.  The latter includes another copy of the executable name
            since that, too, is passed as argv to the executable.
        """
        argv = ['mysql',
                '-u', user,
                '-D', settings.SGX_MYSQL_DB,
                '-h', settings.SGX_MYSQL_HOST,
                '-p' + self.passwords[user]]
        os.execvp('mysql', argv)

    def compare_tables(self, first, second, using, verbose=False):
        def get_result(cursor):
            if verbose:
                sys.stderr.write(cursor.statement + '\n')
            return cursor.fetchone()[0]

        with self.cursor(user='reader') as cursor:
            cursor.execute('SELECT COUNT(*) FROM `{table}`'.format(table=first))
            first_count = get_result(cursor)

            cursor.execute('SELECT COUNT(*) FROM `{table}`'.format(table=second))
            second_count = get_result(cursor)

            if using == 'NA':
                return {
                    '#(before)': first_count,
                    '#(after)': second_count,
                }

            cursor.execute("""
                SELECT COUNT(*)
                FROM `{first}` first
                LEFT OUTER JOIN `{second}` second
                USING ({using})
                WHERE second.{using} IS NULL;
            """.format(first=first, second=second, using=using))
            not_in_second = get_result(cursor)

            cursor.execute("""
                SELECT COUNT(*)
                FROM `{first}` first
                RIGHT OUTER JOIN `{second}` second
                USING ({using})
                WHERE first.{using} IS NULL;
            """.format(first=first, second=second, using=using))
            not_in_first = get_result(cursor)

            return {
                '#(before)': first_count,
                '#(after)': second_count,
                '#(lost)': not_in_second,
                '#(gained)': not_in_first,
            }


MYSQL = _MySQL(profile=settings.SGX_MYSQL_PROFILE)
