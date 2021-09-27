import sys
import logging
import argparse
import fnmatch
from abc import ABC
from abc import abstractmethod
from collections import OrderedDict
import networkx as nx

from . import logger, BiodbError, AbstractAttribute
from . import registry
from .mysql import MYSQL
from .mysql import READER
from .db import ImportedTable, imported_tables


def all_table_datasets(tag):
    # Returns all table datasets with specified tag. If tag is None, returns all
    classes = []
    for cls in registry.TYPE_REGISTRY.values():
        if (issubclass(cls, ImportedTable)):
            if tag is None or tag in cls.tags:
                classes.append(cls)
    return classes


def glob_datasets(glob, tables_only=False):
    classes = []
    for cls in registry.TYPE_REGISTRY.values():
        if tables_only and not issubclass(cls, ImportedTable):
            continue

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
        print('\n'.join(sorted(registry.TYPE_REGISTRY.keys(), key=lambda x: x.lower())))


class DagCommand(AppCommand):
    name = "dag"
    help = "render the dependency DAG of biodb as per current state of code"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('-o', '--output', required=True, help='Path to output png file')
        self.parser.add_argument('-s', '--subgraph', nargs='+', help='Only build the relevant subgraph for these datasets')
        self.parser.add_argument('-a', '--all-types', action='store_true', help='Include all datasets, not just tables')

    def _build_graph(self):
        G = nx.DiGraph()

        def dataset_has_type_of_interest(dataset):
            return self.app.args.all_types or issubclass(dataset, ImportedTable)

        for dataset in registry.TYPE_REGISTRY.values():
            if not dataset_has_type_of_interest(dataset):
                continue

            G.add_node(dataset.__name__)

            for dep in dataset.depends:
                if not dataset_has_type_of_interest(dep):
                    continue

                G.add_node(dep.__name__)
                G.add_edge(dep.__name__, dataset.__name__)

        if self.app.args.subgraph:
            includes = set(sum([
                [cls.__name__ for cls in glob_datasets(node, tables_only=(not self.app.args.all_types))]
                for node in self.app.args.subgraph
            ], []))
            descendants = set().union(*[
                nx.algorithms.dag.descendants(G, node)
                for node in includes
            ])
            ancestors = set().union(*[
                nx.algorithms.dag.ancestors(G, node)
                for node in includes
            ])
            to_keep = set().union(includes, ancestors, descendants)
            nodes = list(G.nodes().keys())
            for node in nodes:
                if node not in to_keep:
                    G.remove_node(node)

            nx.set_node_attributes(G, {
                node: '#0c97ae' if node in includes else '#dddddd'
                for node in G.nodes()
            }, 'color')


        return G

    def run(self):
        # this requires: graphviz and libgraphiz-dev (apt) and pygraphviz (pip)
        G = self._build_graph()
        P = nx.drawing.nx_agraph.to_agraph(G)

        # attributes reference: https://www.graphviz.org/doc/info/attrs.html
        # P.graph_attr['size'] = 2000
        P.graph_attr['margin'] = 2
        P.graph_attr['fontname'] = 'monospace'

        P.graph_attr['rankdir'] = 'LR'
        P.graph_attr['ranksep'] = 2
        P.graph_attr['nodesep'] = 1

        P.node_attr['shape'] = 'box'
        P.node_attr['fontname'] = 'monospace'
        P.node_attr['fontsize'] = 12
        P.node_attr['margin'] = .1

        P.edge_attr['fontsize'] = 8
        P.edge_attr['penwidth'] = 0.5
        P.edge_attr['pencolor'] = '#888888'
        P.edge_attr['arrowhead'] = 'vee'

        P.layout(prog='dot')
        P.draw(self.app.args.output)


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
            ('version',      8),
            ('latest',       7),
            ('Table',        50),
            ('depends',      40),
        ])
        columns = width_by_column.keys()
        fmt_string = ''.join('{%s:%d}' % (col, width) for col, width in width_by_column.items())

        # header line
        print(fmt_string.format(**dict(zip(columns, columns))))

        # content lines
        class_names = [cls.__name__ for cls in glob_datasets(self.app.args.dataset, tables_only=True)]

        for hdataset in imported_tables():
            if hdataset.type not in class_names:
                continue
            row = [
                hdataset.type,
                hdataset.formula['version'],
                hdataset.is_latest(),
                hdataset.name,
                '[' + ', '.join(ds.type for ds in hdataset.inputs.values()) + ']',
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
