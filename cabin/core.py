import json
import hashlib
from abc import ABC, abstractmethod
from collections import OrderedDict

from humanize import naturalsize

from . import logger
from .mysql import MYSQL
from .settings import CABIN_SYSTEM_TABLE


def calculate_sha(obj, num_chars=8):
    m = hashlib.sha256()
    m.update(obj.encode('utf-8'))
    return m.hexdigest()[:num_chars]


class Dataset(ABC):
    """Dataset classes represent datatset types and Dataset instances represent
    dataset instances. All you need to do to implement a new Dataset class is
    define: version, depends, exists(), and produce().

    Class Attributes to be defined:

        version (str):
            The current version of this dataset, reflecting the current state
            of its internal mechanics (i.e. class code).
        depends (list):
            The dataset type's dependencies: a list of other Dataset classes.


    Abstract methods to be implemented:

        exists():
            Whether this Dataset instance currently exists.

        produce():
            Produce a copy of this Dataset instance; assumes all its
            dependencies already exist().

    Optional methods:

        check():
            Performs any kind of consistency check when the dataset is
            produced. Expected to raise in case of errors.

    -----------------

    Available attributes and methods:

        inputs (dict):
            Corresponding to `depends` of the class. A dictionary with
            identical keys as `depends` of class and values being corresponding
            Dataset *instances*.

        name (str):
            A unique, intelligably serialized identifier for this Dataset instance.

        formula (dict):
            The version formula for this Dataset instance, including its
            ancestry all the way to root Datasets.

        produce_recursive():
            produce this instance if it does not already exist(). Makes no
            assumption about the existence of its dependencies.
    """
    version = None
    depends = []

    tags = []

    @classmethod
    def assert_class_attributes(cls, type_, *attrs):
        # TODO make this a class decorator
        for attr in attrs:
            assert isinstance(getattr(cls, attr), type_), \
                'Dataset class "{name}": bad "{attr}"'.format(name=cls.__name__, attr=attr)

    def __init__(self):
        # NOTE all attrs are expected to be RO from outside
        self.inputs = OrderedDict([
            (klass.__name__, klass()) for klass in self.depends
        ])
        if len(self.inputs) == 1:
            # for convenience, make the sole input of a Dataset instance
            # accessible as self.input
            self.input = list(self.inputs.values())[0]

        self.formula = OrderedDict([
            ('type', self.type),
            ('version', self.version),
            ('inputs', OrderedDict([
                (key, inp.formula)
                for key, inp in self.inputs.items()
            ])),
        ])
        self.formula_json = json.dumps(self.formula)
        self.formula_sha = calculate_sha(self.formula_json)

    @abstractmethod
    def exists(self):
        """Checks whether this Dataset already exists, and hence does not need
        to be produced. This function is assumed to be relatively fast, network
        look ups to be cached if necessary."""
        pass

    @abstractmethod
    def produce(self):
        """Produces this Dataset: a downloadable file will be downloaded, an
        importable table will be imported, etc.

        This function assumes that all immediate dependencies of this Dataset
        already exist() and that this Dataset does not already exist().
        """
        pass

    # ============= Internal Mehtods ==============
    # Everything below is supposed to Just Work. Subclasses shouldn't (need to)
    # override any of them.
    def produce_recursive(self, dry_run=False):
        if self.exists():
            logger.debug('exists:'.ljust(9) + self.description)
        else:
            for inp in self.inputs.values():
                # recurse
                inp.produce_recursive(dry_run=dry_run)

            logger.info('produce:'.ljust(10) + self.description)

            if not dry_run:
                self.produce()

                if hasattr(self, 'check'):
                    logger.info('check:'.ljust(10) + self.description)
                    self.check()

                logger.info('produced:'.ljust(10) + self.description)

    def root_versions(self):
        # returns a list of versions of all root ancestors. Root ancestors are
        # always external to cabin (e.g. source dataset like StormDetails.csv). The only
        # reason to use this is to make paths and table names more intelligble.
        if self.is_root:
            return [self.version]

        return sum(
            (inp.root_versions() for _, inp in sorted(self.inputs.items())),
            []
        )

    @classmethod
    def rdepends(cls):
        from cabin import registry
        return [
            klass
            for klass in registry.TYPE_REGISTRY.values()
            if cls in klass.depends
        ]

    @property
    def type(self):
        return self.__class__.__name__

    @property
    def name(self):
        # A unique representation of the Dataset's version formula. To be used
        # as building block of paths or table names. Everything but the formula
        # sha is only included for intelligibility.
        return '{type}::{roots}::{sha}'.format(
            type=self.type,
            roots='::'.join(self.root_versions()[:2]),
            sha=self.formula_sha,
        )

    @property
    def is_root(self):
        return not self.inputs

    @property
    def description(self):
        # A human readable description for the dataset
        return '{type} with formula sha {sha} and root versions {roots}'.format(
            type=self.type.ljust(30),
            # this is the human readable version, set-ify
            roots=', '.join(set(self.root_versions())),
            sha=self.formula_sha,
        )

    def __eq__(self, other):
        return self.formula_sha == other.formula_sha


class HistoricalDataset:
    # FIXME this is not a generic historical dataset; this is specifically an
    # imported table, see db.ImportedTable
    def __init__(self, formula, name=None, sha=None):
        self.type = formula['type']
        self.version = formula['version']
        self.name = name

        self.formula = formula
        self.formula_json = json.dumps(formula)
        self.formula_sha = calculate_sha(self.formula_json)

        if sha:
            assert self.formula_sha == sha, 'Bad SHA %s (expected %s)' % (self.formula_sha, sha)

        # recurse
        self.inputs = {
            name: HistoricalDataset(sub)
            for name, sub in formula['inputs'].items()
        }

    @property
    def is_root(self):
        return not self.inputs

    def root_versions(self):
        if self.is_root:
            return [self.version]
        return list(set(
            sum((inp.root_versions() for _, inp in sorted(self.inputs.items())), [])
        ))

    def is_latest(self):
        """Whether this HistoricalDataset matches the current state of code;
        this is verified by a formula sha comparison.

        Historical datasets that are not "latest" are "stale": they can safely be
        removed from cabin storage. Note that HistoricalDataset is recursive,
        mirroring formulae: the subformulae of a formula are resurrected as
        HistoricalDataset for the dependency dataset. This allows us to do
        garbage collection at all levels (tables, downloaded files,
        intermediate tables).
        """
        from .registry import TYPE_REGISTRY
        type_ = self.formula['type']
        if type_ not in TYPE_REGISTRY:
            # don't even know who this dataset is
            return False

        cls = TYPE_REGISTRY[type_]
        latest = cls()
        return latest.formula_sha == self.formula_sha

    # TODO unify with ImportedTable
    def sql_drop_table(self):
        return 'DROP TABLE IF EXISTS `{table}`;'.format(table=self.name)

    def sql_drop_from_system(self):
        return 'DELETE FROM `{system}` WHERE name="{table}";'.format(system=CABIN_SYSTEM_TABLE, table=self.name)

    def drop(self):
        with MYSQL.cursor() as cursor:
            cursor.execute(self.sql_drop_table())
            cursor.execute(self.sql_drop_from_system())

    def get_data_stats(self):
        with MYSQL.cursor() as cursor:
            cursor.execute("""
                SELECT data_length
                FROM information_schema.tables
                WHERE table_name = '{table}';
            """.format(table=self.name))
            # data_length in information_schema is only an estimate
            data_length = cursor.fetchone()[0]

            cursor.execute('SELECT count(*) FROM `{table}`;'.format(table=self.name))
            # caution: don't use the table_rows column of information_schema,
            # it's approximate (can be very misleading) and we have an easy way
            # to get exact values.
            n_rows = cursor.fetchone()[0]

        return {
            'n_rows': '{:,}'.format(n_rows), # add thousands comma separator
            'size': naturalsize(data_length)
        }
