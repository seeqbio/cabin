import sys
import logging
import argparse
import fnmatch
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
from biodb.data.db import ImportedTable
from biodb.data.registry import load_table_registry


def all_table_datasets(tag):
    # Returns all table datasets with specified tag. If tag is None, returns all
    classes = []
    for cls in registry.TYPE_REGISTRY.values():
        if (issubclass(cls, ImportedTable)):
            if tag is None or tag in cls.tags:
                classes.append(cls)
    return classes


def glob_table_datasets(glob):
    classes = []
    for cls in registry.TYPE_REGISTRY.values():
        if (issubclass(cls, ImportedTable)):
            if fnmatch.fnmatch(cls.__name__, glob):
                classes.append(cls)
    return classes


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
        logger.info('\n'.join(c for c in registry.TYPE_REGISTRY))


class DropUsersCommand(AppCommand):
    name = "drop-users"
    help = "inverse of 'init', drops all users and their privileges, database itself stays put."

    def run(self):
        MYSQL.drop_users()


class DropCommand(AppCommand):
    name = "drop"
    help = "inverse of 'import', drops table from db and `system` table."

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset')
        self.parser.add_argument('-n', '--dry-run', action='store_true', help="show what would be dropped")

    def run(self):
        for hdataset in load_table_registry():
            if fnmatch.fnmatch(hdataset.name, self.app.args.dataset):
                if (self.app.args.dry_run):
                    logger.info("(dry-run) Dropping table: %s" % hdataset.name)
                else:
                    logger.info("Dropping table: %s" % hdataset.name)
                    hdataset.drop()
                    logger.info("Dropped table.")


class PruneCommand(AppCommand):
    name = "prune"
    help = "for all not 'latests', drops tables from db and `system` table."

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('-n', '--dry-run', action='store_true', help="show what would be pruned")

    def run(self):
        for hdataset in load_table_registry():
            if not hdataset.is_latest():
                if (self.app.args.dry_run):
                    logger.info("(dry-run) Pruning outdated table: %s" % hdataset.name)
                else:
                    logger.info("Pruning outdated table: %s" % hdataset.name)
                    hdataset.drop()
                    logger.info("Pruned outdated table.")


class ImportCommand(AppCommand):
    name = "import"
    help = "import a dataset into database"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', nargs='?')
        self.parser.add_argument('--all', action='store_true', help="import all table datasets")
        self.parser.add_argument('--tag', help="only import datasets with TAG, only valid with --all")
        self.parser.add_argument('-n', '--dry-run', action='store_true', help="do not actually import, just show a synopsis")

    def run(self):
        if self.app.args.all:
            classes = all_table_datasets(tag=self.app.args.tag)
        else:
            if self.app.args.tag:
                raise BiodbError('--tag is only valid with --all')
            classes = glob_table_datasets(self.app.args.dataset)

        if not classes:
            logger.error("No Tables in registry matching %s." % self.app.args.dataset)
            return 1

        for cls in classes:
            ds = cls()
            ds.produce_recursive(dry_run=self.app.args.dry_run)


class ShellCommand(AppCommand):
    name = "shell"
    help = "open the MySQL command line client"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('--user', '-u', default=READER, help="user to log in to MySQL")

    def run(self):
        MYSQL.shell(self.app.args.user) # execvp to mysql client


class StatusCommand(AppCommand):
    name = "status"
    help = "describe import and archive status of a dataset"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', nargs='?', default='*')

    def run(self):
        def yesno(val):
            return 'yes' if val else 'no'

        width_by_column = OrderedDict([
            ('type',         32),
            ('version',      9),
            ('depends',      38),
            ('latest',       10),
            ('Full name',    50),
        ])
        columns = width_by_column.keys()
        fmt_string = ''.join('{%s:%d}' % (col, width) for col, width in width_by_column.items())

        # header line
        print(fmt_string.format(**dict(zip(columns, columns))))

        # content lines
        class_names = [cls.__name__ for cls in glob_table_datasets(self.app.args.dataset)]

        for hdataset in load_table_registry():
            if hdataset.type not in class_names:
                continue
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
            'prune':            PruneCommand(app=self),
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
