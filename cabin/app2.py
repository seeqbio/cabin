import os
import sys
import yaml
import fnmatch
import logging
import argparse
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict
from pathlib import Path

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

#from biodb.data.datasets.test_dataset import TestDatasetTable  # rm during registry usage
#from biodb.data.datasets.test_dataset import TestDatasetFile  # rm during registry usage
from biodb.data import registry
from biodb.data.registry import TestDatasetTable
from biodb.data.registry import TestDatasetFile

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
    help = "list all datasets for which a handler exists based on registry"

    def run(self):
        print('\n'.join(c for c in registry.TYPE_REGISTRY))


class DropUsersCommand(AppCommand):
    name = "drop-users"
    help = "inverse of 'init', drops all users and their privileges, database itself stays put."

    def run(self):
        MYSQL.drop_users()


class DropCommand(AppCommand):
    name = "drop"
    help = "inverse of 'import', drops table from db and `system` table."  ## FIXME: add drop to rm File

    def run(self):
        MYSQL.cursor.drop_created_tables()

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset')

    def run(self):
        TestDatasetTable().drop()


class ImportCommand(AppCommand):
    name = "import"
    help = "import a dataset into database"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset')

    def run(self):
        ds = getattr(registry, self.app.args.dataset)()
        ds.produce_recursive()

class ShellCommand(AppCommand):
    name = "shell"
    help = "open the MySQL command line client"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('--user', '-u', default=READER, help="user to log in to MySQL")

    def run(self):
        """Executes mysql client in _this_ process (replaces python process
        immediately). This is necessary; using a subprocess produces weird
        behavior with how interrupts are handled, e.g. how ctrl+c works.

        Note on `os.execvp` usage:
            The first argument to execvp is the executable name (searched for
            in $PATH) and the second argument is the argv list passed to the
            process.  The latter includes another copy of the executable name
            since that, too, is passed as argv to the executable.
        """
        user = self.app.args.user
        MYSQL.shell(self.app.args.user) # execvp to mysql client


class StatusCommand(AppCommand):
    name = "status"
    help = "describe import and archive status of a dataset"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', nargs='?')#, help=self.dataset_help)

    def run(self):
        def yesno(val):
            return 'yes' if val else 'no'

        width_by_column = OrderedDict([
            ('name',         45),
            ('version',      15),
            ('formula sha',  15),
            ('requirements', 22),
#            ('latest',       10),  # TODO: implement this
        ])
        columns = width_by_column.keys()
        fmt_string = ''.join('{%s:%d}' % (col, width) for col, width in width_by_column.items())

        # header line
        print(fmt_string.format(**dict(zip(columns, columns))))

        # content lines
        token = self.app.args.dataset
        for cls in registry.TYPE_REGISTRY:
            dataset = getattr(registry, cls)()
            if token is not None and cls.name[:len(token)] != token:
                continue
            row = [
                dataset.name,
                dataset.version,
                dataset.formula_sha,
                list(dataset.depends.keys()),
        #        yesno(dataset.latest), # not implemented yet
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

        self.commands = {
            'shell':    ShellCommand(app=self),
            'init':     InitCommand(app=self),
            'list':     ListCommand(app=self),
            'drop-users':     DropUsersCommand(app=self),
            'import':     ImportCommand(app=self),
            'drop':        DropCommand(app=self),
            'status':        StatusCommand(app=self)

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
