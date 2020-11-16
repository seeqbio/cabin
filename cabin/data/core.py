import json
import hashlib
from pathlib import Path
from abc import ABC, abstractmethod
from collections import OrderedDict


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
        depends (dict):
            The dataset type's named dependencies: a dictionary from dependnecy
            key (anything for your convenience) to dependecy Dataset class.


    Abstract methods to be implemented:

        exists():
            Whether this Dataset instance currently exists.

        produce():
            Produce a copy of this Dataset instance; assumes all its
            dependencies already exist().

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
    depends = {}

    def __new__(cls):
        # for convenience allow depends to be defined as a single other Dataset
        # (most common: single input), convert that to {'other': other} here.
        import inspect
        if inspect.isclass(cls.depends) and issubclass(cls.depends, Dataset):
            cls.depends = {cls.__name__: cls.depends}
        cls.validate_class()
        return super(Dataset, cls).__new__(cls)

    @classmethod
    def validate_class(cls):
        cls.assert_class_attributes(dict, 'depends')
        cls.assert_class_attributes(str, 'version')

    @classmethod
    def assert_class_attributes(cls, type_, *attrs):
        # TODO make this a class decorator
        for attr in attrs:
            assert isinstance(getattr(cls, attr), type_), \
                'Dataset class "{name}": bad "{attr}"'.format(name=cls.__name__, attr=attr)

    def __init__(self):
        # NOTE all attrs are expected to be RO from outside
        self.inputs = OrderedDict([
            (name, klass()) for name, klass in sorted(self.depends.items())
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

    def produce_recursive(self):
        if self.exists():
            print('--> already exists: %s' % self.description)
        else:
            print('--> to be produced: %s' % self.description)

            for inp in self.inputs.values():
                # recurse
                inp.produce_recursive()

            print('--> producing:      %s' % self.description)
            self.produce()

    def root_versions(self):
        # returns a list of versions of all root ancestors. Root ancestors are
        # always external to biodb (e.g. source VCF for ClinVar). The only
        # reason to use this is to make paths and table names more intelligble.
        if self.is_root:
            return [self.version]
        return list(set(
            sum((inp.root_versions() for _, inp in sorted(self.inputs.items())), [])
        ))

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
            roots='::'.join(self.root_versions()),
            sha=self.formula_sha,
        )

    @property
    def is_root(self):
        return not self.inputs

    @property
    def description(self):
        # A human readable description for the dataset
        return '{type} with formula sha {sha} and root versions {roots}'.format(
            type=self.type.ljust(25),
            roots=', '.join(self.root_versions()),
            sha=self.formula_sha,
        )

    def __eq__(self, other):
        return self.formula_sha == other.formula_sha


class HistoricalDataset:
    def __init__(self, formula, name=None, sha=None):
        self.type = formula['type']
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

    def is_latest(self):
        """Whether this HistoricalDataset matches the current state of code;
        this is verified by a formula sha comparison.

        Historical datasets that are not "latest" are "stale": they can safely be
        removed from biodb storage. Note that HistoricalDataset is recursive
        mirroring formulae: the subformulae of a formula are resurrected as
        HistoricalDatasets for the dependency dataset. This allows us to do
        garbage collection at all levels (tables, downloaded files,
        intermediate tables).
        """
        from biodb.data.registry import TYPE_REGISTRY
        type_ = self.formula['type']
        if type_ not in TYPE_REGISTRY:
            # don't even know who this dataset is
            return False

        cls = TYPE_REGISTRY[type_]
        latest = cls()
        return latest.formula_sha == self.formula_sha
