import os
import sys
import types
import getpass
import textwrap
import mysql.connector
from contextlib import contextmanager

from biodb import logger
from biodb import settings


READER = 'reader'
WRITER = 'writer'


class _MySQL:
    passwords = {'reader': settings.SGX_MYSQL_READER_PASSWORD,
                 'writer': settings.SGX_MYSQL_WRITER_PASSWORD}

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

    @contextmanager
    def connection(self, user, **kw):
        cnx_kw = {
            'user': user,
            'password': self.passwords[user],
            'host': settings.SGX_MYSQL_HOST,
            'database': settings.SGX_MYSQL_DB,
        }
        cnx_kw.update(**kw)
        cnx = mysql.connector.connect(**cnx_kw)
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
                        print('  {time}{query}'.format(idx=idx, time=time_ms, query=query))
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

    # NOTE we should get rid of anonymous users, either install 5.7 +
    # (not available on our current RHEL AMI, but available on Ubuntu 16.04) or
    # run mysql_secure_installation and remove anonymous users, cf.
    # https://stackoverflow.com/a/1412356

    # NOTE to move the mysql data directory, set `datadir` (under mysqld) in
    #   * /etc/my.cnf in RHEL
    #   * /etc/mysql/mysql.conf.d/mysql.cnf in Ubuntu
    def initialize(self):
        database = settings.SGX_MYSQL_DB

        root_password = getpass.getpass("Enter root password (given to you): ")

        cnx = mysql.connector.connect(user='root',
                                      host=settings.SGX_MYSQL_HOST,
                                      password=root_password)
        cursor = cnx.cursor()

        cursor.execute('CREATE DATABASE IF NOT EXISTS {db};'.format(db=database))
        logger.info('created database "%s"' % database)

        def _grant(grant, user):
            q_tpl = 'GRANT {grant} ON `{db}`.* TO "{user}"@"%" IDENTIFIED BY "{password}";'
            q = q_tpl.format(grant=grant, db=database, user=user, password=self.passwords[user])

            cursor.execute(q)
            logger.info('granted "{g}" to user "{u}"'.format(g=grant, u=user))

        _grant('SELECT', 'reader')
        _grant('ALL PRIVILEGES', 'writer')
        cursor.execute('FLUSH PRIVILEGES;')
        cursor.close()

        logger.info('successfully initialized "{db}"!'.format(db=database))

    def shell(self, user):
        argv = ['mysql',
                '-u', user,
                '-D', settings.SGX_MYSQL_DB,
                '-p' + self.passwords[user]]
        os.execvp('mysql', argv)


MYSQL = _MySQL(profile=settings.SGX_MYSQL_PROFILE)
