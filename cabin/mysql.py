import sys
import types
import textwrap
import mysql.connector
from contextlib import contextmanager

from biodb.io import log


READER = 'reader'
WRITER = 'writer'


class MySQL:
    def __init__(self, config, debug=False):
        self.config = config
        self.debug = debug

    def debuglog(self, message, file=sys.stderr):
        if self.debug:
            log(message, header='[DEBUG] ')

    @contextmanager
    def transaction(self, connection_kw={}, cursor_kw={}):

        def create_table(cursor, table_name, query):
            cursor._created_tables.append(table_name)
            cursor.execute(query)

        def drop_created_tables(cursor):
            for table in cursor._created_tables:
                cursor.execute('DROP TABLE IF EXISTS `%s`;' % table)

        with self.connection('writer', **connection_kw) as cnx:
            cnx.start_transaction()
            with cnx.cursor(**cursor_kw) as cursor:
                cursor._created_tables = []
                cursor.create_table = types.MethodType(create_table, cursor)
                cursor.drop_created_tables = types.MethodType(drop_created_tables, cursor)
                try:
                    yield cursor
                    cnx.commit()
                except: # catches all exceptions, even user-initiated ctrl + C
                    cursor.drop_created_tables()
                    cnx.rollback()
                    raise

    @contextmanager
    def connection(self, user, **kw):
        cnx_kw = {
            'user': user,
            'password': self.config['passwords'][user],
            'host': 'localhost',
            'database': self.config['database'],
        }
        cnx_kw.update(**kw)
        cnx = mysql.connector.connect(**cnx_kw)
        real_cursor = cnx.cursor

        @contextmanager
        def cursor_wrapper(*args, **kwargs):

            cursor = real_cursor(cnx, *args, **kwargs)
            if self.debug:
                cursor.execute('SET profiling = 1')

            try:
                yield cursor
            finally:
                if self.debug:
                    cursor.execute('SHOW profiles;')
                    for row in cursor:
                        idx, time_s, query = row
                        time_ms = '{t} ms'.format(t=round(time_s * 1000, 2)).ljust(10)
                        query = '\n\t\t\t'.join(textwrap.wrap(query, 80))
                        self.debuglog('{time}{query}'.format(idx=idx, time=time_ms, query=query))
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
        database = self.config['database']

        cnx = mysql.connector.connect(user='root',
                                      password=self.config['passwords']['root'])
        cursor = cnx.cursor()

        cursor.execute('CREATE DATABASE IF NOT EXISTS {db};'.format(db=database))
        log('created database "%s"' % database)

        def _grant(grant, user):
            q_tpl = 'GRANT {grant} ON `{db}`.* TO "{user}"@"%" IDENTIFIED BY "{password}";'
            q = q_tpl.format(grant=grant, db=database, user=user,
                             password=self.config['passwords'][user])

            cursor.execute(q)
            log('granted "{g}" to user "{u}"'.format(g=grant, u=user))

        _grant('SELECT', 'reader')
        _grant('ALL PRIVILEGES', 'writer')
        cursor.execute('FLUSH PRIVILEGES;')
        cursor.close()

        log('successfully initialized "{db}"!'.format(db=database))

    def num_records(self, table):
        if not self.tables(table):
            return None
        with self.connection('reader') as cnx:
            cursor = cnx.cursor()
            cursor.execute('SELECT COUNT(*) FROM `{t}`;'.format(t=table))
            return cursor.fetchone()[0]
