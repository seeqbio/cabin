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

from biodb.base import DatasetVersion
from biodb.datasets.test_dataset import test_dataset
from biodb.datasets.gene_info import gene_info
from biodb.datasets.refseq_summary import refseq_summary
from biodb.datasets.gene2refseq import gene2refseq
from biodb.datasets.gene2ensembl import gene2ensembl
from biodb.datasets.dbSNP import dbSNP
from biodb.datasets.dbNSFP import dbNSFP
from biodb.datasets.ClinVar import ClinVarVCF
from biodb.datasets.ClinVar import ClinVarHGVS4Variation
from biodb.datasets.ClinVar import ClinVarSCV
from biodb.datasets.ClinVar import ClinVarRCV
from biodb.datasets.ensembl import ENSEMBL_DATASETS
from biodb.datasets.CancerMine import CancerMineCollated
from biodb.datasets.CancerMine import CancerMineSentences
from biodb.datasets.CIViC import CIViC
from biodb.datasets.hpo import hpo
from biodb.datasets.disease_ontology import DiseaseOntology
from biodb.datasets.mondo import mondo
from biodb.datasets.pmkb import pmkb
from biodb.datasets.pfam import Pfam
from biodb.datasets.pharmGKB import pharmGKB
from biodb.datasets.dgi import dgi
from biodb.datasets.medgen import medgenNames
from biodb.datasets.medgen import medgenDefn
from biodb.datasets.medgen import medgenHPO


class UsageError(Exception):
    pass


class AppCommand(ABC):
    name = AbstractAttribute()
    help = None
    description = None

    dataset_help = 'The internal name for a dataset, e.g. "ClinVar"'
    source_version_help = 'The version of a dataset as represented by data source'
    checksum_help = 'The sha256 checksum of downloaded file contents'
    git_version_help = 'The version of biodb used for importing a dataset'

    def __init__(self, app=None):
        description = self.description if self.description else self.help
        self.parser = app.cmd_parser.add_parser(self.name, description=description, help=self.help)
        self.app = app

    @abstractmethod
    def run(self):
        pass


class InitCommand(AppCommand):
    name = "init"
    help = "initialize databases and users"

    def run(self):
        MYSQL.initialize()


class DropUsersCommand(AppCommand):
    name = "drop-users"
    help = "inverse of 'init', drops all users and their privileges, database itself stays put."

    def run(self):
        MYSQL.drop_users()


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
        MYSQL.shell(self.app.args.user) # execvp to mysql client


class ListCommand(AppCommand):
    name = "list"
    help = "list all datasets for which a handler exists"

    def run(self):
        classes = [cls.name for cls in self.app.dataset_classes]
        print('\n'.join(classes))


class ImportCommand(AppCommand):
    name = "import"
    help = "import a dataset into database"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', help=self.dataset_help + '; accepts glob, e.g. "ensembl.*"')
        self.parser.add_argument('source_version', help=self.source_version_help)
        self.parser.add_argument('checksum', nargs='?', help=self.checksum_help)

    def run(self):
        # fnmatch is the internal mechanism used by glob; currently only
        # works for "ensembl.*" (and not, say, "gene2ref*")
        classes = [ds_class for ds_class in self.app.dataset_classes
                   if fnmatch.fnmatch(ds_class.name, self.app.args.dataset)]
        if not classes:
            logger.error('unknown dataset pattern "%s"' % self.app.args.dataset)
            return 1
        for ds_class in classes:
            ds_name = ds_class.name
            ds_version = DatasetVersion(source=self.app.args.source_version,
                                        checksum=self.app.args.checksum)
            ds = self.app.dataset(name=ds_name, version=ds_version)
            ds.import_()
            ds.archive()


class DropCommand(AppCommand):
    name = "drop"
    help = "drop a dataset from database"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', help=self.dataset_help)
        self.parser.add_argument('source_version', help=self.source_version_help)
        self.parser.add_argument('checksum', help=self.checksum_help)
        self.parser.add_argument('git_version', help=self.git_version_help)

    def run(self):
        ds_version = DatasetVersion(source=self.app.args.source_version,
                                    checksum=self.app.args.checksum,
                                    git=self.app.args.git_version)
        ds = self.app.dataset(name=self.app.args.dataset, version=ds_version)
        ds.drop()


class StatusCommand(AppCommand):
    name = "status"
    help = "describe import and archive status of a dataset"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', nargs='?', help=self.dataset_help)

    def run(self):
        def yesno(val):
            return 'yes' if val else 'no'

        width_by_column = OrderedDict([
            ('name',        27),
            ('version',     15),
            ('checksum',    12),
            ('code',        30),
            ('download',    10),
            ('archive',     10),
            ('import',      10),
        ])
        columns = width_by_column.keys()
        fmt_string = ''.join('{%s:%d}' % (col, width) for col, width in width_by_column.items())

        # header line
        print(fmt_string.format(**dict(zip(columns, columns))))

        # content lines
        token = self.app.args.dataset
        for cls in self.app.dataset_classes:
            if token is not None and cls.name[:len(token)] != token:
                continue

            for dataset in cls.search_sorted():
                row = [
                    dataset.name,
                    dataset.version.source,
                    dataset.version.checksum,
                    dataset.version.git,
                    yesno(dataset.downloaded),
                    yesno(dataset.archived),
                    yesno(dataset.imported)
                ]
                row = [str(x) if x else '' for x in row]
                print(fmt_string.format(**dict(zip(columns, row))))


class DownloadCommand(AppCommand):
    name = "download"
    help = "download a dataset from source or archives"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', help=self.dataset_help)
        self.parser.add_argument('source_version', help=self.source_version_help)
        self.parser.add_argument('checksum', nargs='?', help=self.checksum_help)

    def run(self):
        version = DatasetVersion(source=self.app.args.source_version,
                                 checksum=self.app.args.checksum)
        ds = self.app.dataset(name=self.app.args.dataset, version=version)
        ds.download()


class ArchiveCommand(AppCommand):
    name = "archive"
    help = "update archives for a dataset"

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.parser.add_argument('dataset', help=self.dataset_help)
        self.parser.add_argument('source_version', help=self.source_version_help)
        self.parser.add_argument('checksum', help=self.checksum_help)

    def run(self):
        version = DatasetVersion(source=self.app.args.source_version,
                                 checksum=self.app.args.checksum)
        ds = self.app.dataset(name=self.app.args.dataset, version=version)
        ds.archive()


class App:
    dataset_classes = [
        CancerMineCollated,
        CancerMineSentences,
        test_dataset,
        gene_info,
        refseq_summary,
        gene2refseq,
        gene2ensembl,
        ClinVarVCF,
        ClinVarHGVS4Variation,
        ClinVarSCV,
        ClinVarRCV,
        CIViC,
        dbNSFP,
        dbSNP,
        hpo,
        DiseaseOntology,
        mondo,
        pmkb,
        Pfam,
        pharmGKB,
        dgi,
        medgenNames,
        medgenDefn,
        medgenHPO
    ] + list(ENSEMBL_DATASETS.values())

    def dataset(self, name, version):
        for cls in self.dataset_classes:
            if cls.name == name:
                return cls(version=version)
        else:
            raise BiodbError('Unknown dataset ' + name)

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
            'import':   ImportCommand(app=self),
            'drop':     DropCommand(app=self),
            'status':   StatusCommand(app=self),
            'download': DownloadCommand(app=self),
            'archive':  ArchiveCommand(app=self),
            'drop-users':   DropUsersCommand(app=self),
        }

    def init(self, *argv):
        self.args = self.parser.parse_args(argv)
        if self.args.debug:
            logger.setLevel(logging.debug)

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
