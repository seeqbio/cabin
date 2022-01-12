import sys
import logging
import argparse
import fnmatch
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict

import prettytable

from . import logger, BiodbError, AbstractAttribute
from . import registry
from .mysql import MYSQL
from .db import ImportedTable, imported_tables
from .graph import glob_datasets, build_historical_dag, draw_code_dag


def all_table_datasets(tag):
    # Returns all table datasets with specified tag. If tag is None, returns all
    classes = []
    for cls in registry.TYPE_REGISTRY.values():
        if (issubclass(cls, ImportedTable)):
            if tag is None or tag in cls.tags:
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
        print('\n'.join(sorted(registry.TYPE_REGISTRY.keys(), key=lambda x: x.lower())))


class DagCommand(AppCommand):
    # this command requires: graphviz and libgraphiz-dev (apt) and pygraphviz (pip)
    name = "dag"
    help = "render the dependency DAG of biodb as per current state of code"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('-o', '--output', required=True, help='Path to output png file')
        self.parser.add_argument('-s', '--subgraph', nargs='+', help='Only build the relevant subgraph for these datasets')
        self.parser.add_argument('-a', '--all-types', action='store_true', help='Include all datasets, not just tables')

    def run(self):
        draw_code_dag(
            path=self.app.args.output,
            tables_only=(not self.app.args.all_types),
            nodes_of_interest_glob=self.app.args.subgraph,
        )


class DescribeCommand(AppCommand):
    name = "describe"
    help = "describe the SQL schema of an imported table"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset')

    def run(self):
        ds_type = self.app.args.dataset
        hdatasets = [
            hd for hd in imported_tables()
            if hd.type == ds_type
        ]
        if not hdatasets:
            # nothing found: let exit code still be zero, just print a warning
            logger.warn('No tables imported for dataset "%s"' % ds_type)
            return

        for hdataset in hdatasets:
            print('=> SCHEMA and INDEXES for %s (version=%s)\n' % (hdataset.name, hdataset.formula['version']))
            query = """
                DESCRIBE `{table}`;
                SHOW INDEX FROM `{table}`;
                SHOW TABLE STATUS WHERE name = '{table}' \G
            """.format(table=hdataset.name)
            MYSQL.shell_query(query)


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
        for hdataset in imported_tables():
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
        self.parser.add_argument('dataset', nargs='?', help="optional dataset name, possibly glob")

    def run(self):
        if self.app.args.dataset:
            class_names = [cls.__name__ for cls in glob_datasets(self.app.args.dataset, tables_only=False)]

        for hdataset in imported_tables():
            if self.app.args.dataset and hdataset.type not in class_names:
                continue

            if not hdataset.is_latest():
                if (self.app.args.dry_run):
                    logger.info("(dry-run) Pruning outdated table: %s" % hdataset.name)
                else:
                    logger.info("Pruning outdated table: %s" % hdataset.name)
                    hdataset.drop()


class ImportCommand(AppCommand):
    name = "import"
    help = "import a dataset into database"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', nargs='*', help="one or more dataset names, possibly globs")
        self.parser.add_argument('--all', action='store_true', help="import all table datasets")
        self.parser.add_argument('--tag', help="only import datasets with TAG, only valid with --all")
        self.parser.add_argument('-n', '--dry-run', action='store_true', help="do not actually import, just show a synopsis")

    def run(self):
        if self.app.args.all:
            classes = all_table_datasets(tag=self.app.args.tag)
        else:
            if self.app.args.tag:
                raise BiodbError('--tag is only valid with --all')

            if not self.app.args.dataset:
                raise BiodbError('either specify a dataset or --all')

            classes = sum(
                (glob_datasets(glob) for glob in self.app.args.dataset),
                []
            )

        if not classes:
            logger.error("No datasets matching %s." % self.app.args.dataset)
            return 1

        for cls in classes:
            ds = cls()
            ds.produce_recursive(dry_run=self.app.args.dry_run)


class ShellCommand(AppCommand):
    name = "shell"
    help = "open the MySQL command line client"

    def run(self):
        MYSQL.shell() # execvp to mysql client


class StatusCommand(AppCommand):
    name = "status"
    help = "describe import and archive status of a dataset"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', nargs='*', default='*')

    def _truncate_list(self, strings):
        # given a list of strings, reduces them to 'A B' or 'A ...'
        # used in rendering potentially long list of inputs/outputs
        if not strings:
            truncated_str = ''
        elif len(strings) == 1:
            truncated_str = strings[0]
        else:
            truncated_str = strings[0] + ' ...'

        return '(%d) %s' % (len(strings), truncated_str)

    def run(self):
        ptable = prettytable.PrettyTable()
        ptable.set_style(prettytable.MARKDOWN)
        ptable.field_names = [
            'version',
            'table',
            'rows',
            'size',
            'inputs',
            'outputs',
        ]
        ptable.align = 'l' # default left align
        ptable.align['rows'] = 'r'
        ptable.align['size'] = 'r'
        ptable.align['version'] = 'r'

        class_names = set(sum([
            [cls.__name__ for cls in glob_datasets(glob, tables_only=True)]
            for glob in self.app.args.dataset
        ], []))

        hdatasets = list(imported_tables())
        hdag = build_historical_dag(hdatasets)

        for hdataset in hdatasets:
            ds_type = hdataset.type
            if ds_type not in class_names:
                continue

            data_stats = hdataset.get_data_stats()
            row = [
                hdataset.formula['version'] + ('  ✓' if hdataset.is_latest() else '  !'),
                hdataset.name,
                data_stats['n_rows'],
                data_stats['size'],
                self._truncate_list(list(hdataset.inputs.keys())),
                self._truncate_list(list(hdag.successors(hdataset.name))),
            ]
            ptable.add_row(row)

        print(ptable)


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
            'dag':              DagCommand(app=self),
            'describe':         DescribeCommand(app=self),
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
