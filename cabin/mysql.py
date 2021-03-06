import os
import sys
import types
import textwrap
from time import sleep
import mysql.connector
from contextlib import contextmanager

from . import logger, settings, CabinError


class _MySQL:

    def wait_for_connection(self, retry_every=1, timeout=settings.CABIN_MYSQL_CNX_TIMEOUT, **cnx_kw):
        """Returns a connection to MySQL with the given connection kwargs, but
        retries a number of times before raising an exception.

        Args:
            retry_every (int): # of seconds to wait between trials to connect.
            timeout     (int): # of seconds after which aborts retrying.
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

        sys.stderr.write('\n')
        raise CabinError('Failed to connect to MySQL with %s' % str(last_exception))

    @contextmanager
    def connection(self, user=settings.CABIN_MYSQL_USER, **kw):
        cnx_kw = {
            'user': user,
            'password': self._password_for(user),
            'host': settings.CABIN_MYSQL_HOST,
            'database': settings.CABIN_MYSQL_DB,
        }
        cnx_kw.update(**kw)
        cnx = self.wait_for_connection(**cnx_kw)
        try:
            yield cnx
        finally:
            cnx.close()

    @contextmanager
    def cursor(self, user=settings.CABIN_MYSQL_USER, connection_kw={}, cursor_kw={}):
        connection_kw.setdefault('allow_local_infile', True)
        with self.connection(user=user, **connection_kw) as cnx:
            with cnx.cursor(**cursor_kw) as cursor:
                # allow caller to commit when they wish
                cursor.commit = cnx.commit
                yield cursor

            # since we use the default engine InnoDB, we are _always_ in a
            # transaction. We need to commit before closing the connection or
            # we will lose data. If the user is not READER, the cursor
            # could possibly have been used to write, assume we need to commit.
            # Note: autocommit drives our import performance off a cliff
            cnx.commit()

    def _password_for(self, user):
        if user == settings.CABIN_MYSQL_USER:
            assert settings.CABIN_MYSQL_PASSWORD, 'Unset password CABIN_MYSQL_PASSWORD'
            return settings.CABIN_MYSQL_PASSWORD
        else:
            raise CabinError('No such user: %s' % user)

    def seems_initialized(self):
        with self.cursor() as cursor:
            cursor.execute("""
                SELECT TABLE_NAME
                FROM information_schema.TABLES
                WHERE TABLE_NAME='{system}';
            """.format(system=settings.CABIN_SYSTEM_TABLE))
            system_exists = bool(cursor.fetchall())
        return system_exists

    def initialize(self):
        database = settings.CABIN_MYSQL_DB
        logger.info('initialiazing database "%s"' % database)
        if self.seems_initialized():
            raise CabinError('Database `%s` seems to be already initialized!' % database)

        def _add_system_table():
            logger.info('creating system table')
            with self.cursor() as cursor:
                cursor.execute('USE {db};'.format(db=database))
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS `{system}` (
                        instance_id VARCHAR(64),
                        sha         VARCHAR(64)  PRIMARY KEY,
                        type        VARCHAR(128),
                        name        VARCHAR(255),
                        table_name  VARCHAR(255),
                        formula     TEXT
                    );
                """.format(system=settings.CABIN_SYSTEM_TABLE))

        _add_system_table()

        logger.info('successfully initialized "{db}"!'.format(db=database))

    def shell(self, user=settings.CABIN_MYSQL_USER):
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
                '-D', settings.CABIN_MYSQL_DB,
                '-h', settings.CABIN_MYSQL_HOST,
                '-p' + self._password_for(user)]
        os.execvp('mysql', argv)

    def shell_query(self, query, user=settings.CABIN_MYSQL_USER):
        """Executes mysql client in a subprocess and runs the provided SQL
        statement against it. Stdout/err are not captured and controlled is
        returned to this process."""
        import subprocess
        args = ['mysql',
                '-u', user,
                '-D', settings.CABIN_MYSQL_DB,
                '-h', settings.CABIN_MYSQL_HOST,
                '-p' + self._password_for(user),
                '-b',
                '-e', query
                ]
        subprocess.Popen(args).communicate()

MYSQL = _MySQL()
