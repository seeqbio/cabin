"""
Implements import magic. Usage:

    from biodb.registry import CIViCTable
    # just works, regardless of where in biodb.datasets the class is defined

    from biodb.registry import TYPE_REGISTRY
    TYPE_REGISTRY['CIViCTable'] == CIViCTable   # True
"""
from typing import Dict, Type
import pkgutil
import inspect
import importlib

from .core import Dataset


def get_class_path(cls):
    """Given a class object, returns the absolute path to the module that defines it."""
    return inspect.getmodule(cls).__file__


def import_dataset_classes(root_name: str='cabin.datasets') -> Dict[str, Type]:
    """Recursively finds all Dataset classes defined in the root package and
    imports them. Returns a dictionary of Dataset type, i.e. class name, to
    Class object.
    """

    # important implementation detail: we want to always refer to modules we
    # import here in full name all the way to root_name. This means, for
    # example, performing the equivalent of
    #
    #       import biodb.datatasets.CIViC
    #
    # in order to get at the classes in that module, and *not* the equivalent of
    #
    #       from biodb.datasets import CIViC
    #
    # This way we can easily clobber our caller's unrelated imports if they
    # have modules with similar names. After the equivalent of the above
    # relative import gets executed, anyone asking for
    #
    #       import CIViC
    #
    # will get the module we import here.
    pkg = importlib.import_module(root_name)
    classes = {}
    for importer, modname, ispkg in pkgutil.walk_packages(path=pkg.__path__, prefix=root_name + '.'):
        mod = importlib.import_module(modname) # e.g. biodb.datasets.CIViC
        for name, cls in inspect.getmembers(mod):
            if not inspect.isclass(cls):
                continue
            # exclude any Dataset without a proper version (e.g. None).
            # This is a proxy for (abstract) base classes.
            if issubclass(cls, Dataset) and getattr(cls, 'version', None):
                # e.g. CIViCTable from biodb.datasets.CIViC
                name = cls.__name__
                if name in classes:
                    # enforce class name uniqueness but don't choke on the same
                    # class appearing in the symbol table of multiple modules.
                    # For example class C may be defined in module M and
                    # imported in N. Then we ended up with two distinct class
                    # objects (M.C and N.C). To see if they are really the same
                    # compare their originating files:
                    assert get_class_path(cls) == get_class_path(classes[name]), \
                        'Duplicate dataset: %s, %s' % (cls, classes[name])

                classes[name] = cls

    return classes


TYPE_REGISTRY = import_dataset_classes()
for name, cls in TYPE_REGISTRY.items():
    globals()[name] = cls
