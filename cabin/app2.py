import sys
import fnmatch
import logging
import argparse
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict

from biodb import logger
from biodb import BiodbError
from biodb import AbstractAttribute
# NOTE For some unknown reason this segfaults on python 3.5.2 and
# mysql-connector-python 8.0.17:
#
#   >>> import hashlib; import mysql.connector
#
# but this does not:
#   >>> import mysql.connector; import hashlib
#
# This means we should import mysql.connector _before_ hashlib.
from biodb.mysql import MYSQL
from biodb.mysql import READER

from biodb.data import registry
from biodb.data.registry import TestDatasetTable
from biodb.data.registry import TestDatasetFile
from biodb.data.registry import load_table_registry

class AppCommand(ABC):
    name = AbstractAttribute()
    help = None
    description = None

    def __init__(self, app=None):
        description = self.description if self.description else self.help
        self.parser = app.cmd_parser.add_parser(self.name, description=description, help=self.help)
        self.app = app

    @abstractmethod
    def run(self):
        pass


class InitCommand(AppCommand):
    name = "init"
    help = "initialize database, users, and build system table"

    def run(self):
        MYSQL.initialize()


class ListCommand(AppCommand):
    name = "list"
    help = "list all datasets for which a handler exists"

    def run(self):
        print('\n'.join(c for c in registry.TYPE_REGISTRY))


class DropUsersCommand(AppCommand):
    name = "drop-users"
    help = "inverse of 'init', drops all users and their privileges, database itself stays put."

    def run(self):
        MYSQL.drop_users()


class DropCommand(AppCommand):
    name = "drop"
    help = "inverse of 'import', drops table from db and `system` table."

    def run(self):
        MYSQL.cursor.drop_created_tables()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset')

    def run(self):
        ds = getattr(registry, self.app.args.dataset)()
        ds.drop()


class ImportCommand(AppCommand):
    name = "import"
    help = "import a dataset into database"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset')
        self.parser.add_argument('-n', '--dry-run', action='store_true', help="do not actually import, just show a synopsis")
        self.parser.add_argument('--compare', default=None, help="Compare biodb to biodb2 import of the same table")

    def get_tables(self, ds_obj):
        version = ds_obj.depends[0].depends[0].version  # Note the uglyness of version will fail for ftp (one more layer), could use formula 
        root_name = ds_obj.type.split('Table')[0]
        import_old_cmd = ['bin/biodb', 'import', root_name, version] 
        after = ds_obj.table_name
        using = self.app.args.compare

        # locally import before table:
        import subprocess
        print("Locally importing before table", import_old_cmd)
        proc = subprocess.Popen(import_old_cmd, stdout = subprocess.PIPE, stderr = subprocess.PIPE)
        stdout, stderr = proc.communicate()
        before_table_name = root_name + "::" + str(stderr).split("Importing ")[1].split("\\n")[0].split('"')[3]
        msg = proc.returncode
        return before_table_name, after

    def run(self):
        ds = getattr(registry, self.app.args.dataset)()
        # Dev tool for comparison, to be removed after all datasets are ported:
        compare_on = self.app.args.compare
        ds.produce_recursive(dry_run=self.app.args.dry_run)
        if compare_on:
            before, after = self.get_tables(ds)
            # IT IS THIS LINE THAT TRIGGERS ERROR ON L116 if verbose=args.verbose
            comparison = MYSQL.compare_tables(before, after, compare_on)
            print('* before:\t%s\n* after:\t%s\n* using:\t%s' % (before, after, compare_on))
            print('\n'.join(key + ':\t' + str(value) for key, value in comparison.items()))


class ShellCommand(AppCommand):
    name = "shell"
    help = "open the MySQL command line client"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('--user', '-u', default=READER, help="user to log in to MySQL")

    def run(self):
        user = self.app.args.user
        MYSQL.shell(self.app.args.user) # execvp to mysql client


class StatusCommand(AppCommand):
    name = "status"
    help = "describe import and archive status of a dataset"

    def run(self):
        def yesno(val):
            return 'yes' if val else 'no'


        width_by_column = OrderedDict([
            ('type',         25),
            ('version',      10),
            ('depends',      32),
            ('latest',       10),
            ('Full name',    50),
        ])
        columns = width_by_column.keys()
        fmt_string = ''.join('{%s:%d}' % (col, width) for col, width in width_by_column.items())

        # header line
        print(fmt_string.format(**dict(zip(columns, columns))))

        # content lines
        for _, hdataset in sorted(load_table_registry().items()):
            row = [
                hdataset.type,
                hdataset.formula['version'], # TODO: consider adding to historical dataset as atribute 
                list(hdataset.formula['inputs'].keys()), # TODO: consider', '.join(c.__name__ for c in hdataset.depends),
                hdataset.is_latest(),
                hdataset.name,
            ]
            row = [str(x) if x else '' for x in row]
            print(fmt_string.format(**dict(zip(columns, row))))


class App:
    def __init__(self):
        parser = argparse.ArgumentParser(description="""
            Versioned importer of datasets into MySQL with S3 archiving.
        """)
        parser.add_argument('-d', '--debug',
                            default=False, action='store_true',
                            help="Print debugging information, default: False")

        self.cmd_parser = parser.add_subparsers(title='commands', dest='command')
        self.parser = parser

        # TODO consider refactor: stop passing app into commands, pass args to
        # their run()
        self.commands = {
            'shell':            ShellCommand(app=self),
            'init':             InitCommand(app=self),
            'list':             ListCommand(app=self),
            'drop-users':       DropUsersCommand(app=self),
            'import':           ImportCommand(app=self),
            'drop':             DropCommand(app=self),
            'status':           StatusCommand(app=self)

        }

    def init(self, *argv):
        self.args = self.parser.parse_args(argv)
        if self.args.debug:
            logger.setLevel(logging.DEBUG)

    def run(self, *argv):
        self.init(*argv)

        if self.args.command is None:
            self.parser.print_help(file=sys.stderr)
            return 1
        else:
            try:
                return self.commands[self.args.command].run()
            except BiodbError as e:
                logger.error('Error: ' + str(e))
                if self.args.debug:
                    raise
                return 1
