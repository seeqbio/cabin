"""
Implements import magic:

    * Any concrete Dataset subclass defined anywhere in biodb.db.datasets is
      imported and added to the symbol table of this module so that you can do
      things like: `from biodb.registry import MyDatasetClass` regardless of
      where in biodb.data.datasets MyDatasetClass is defined.
    * Provides TYPE_REGISTRY which is a {cls.name => cls} dictionary of all
      defined Dataset classes.
    * Provides TABLE_REGISTRY which is a {dataset.name => dataset} dictionary of
      all datasets that are imported in the database.
"""
import pkgutil
import inspect

import biodb.data.datasets
from .core import Dataset
from .db import imported_datasets


def get_class_path(cls):
    # given a class object, returns the absolute path to the module that defines it
    return inspect.getmodule(cls).__file__


def load_dataset_classes(pkg):
    # recursively finds all datasets defined in the biodb.datasets package
    classes = {}
    for importer, modname, ispkg in pkgutil.walk_packages(pkg.__path__):
        mod = importer.find_module(modname).load_module(modname)
        for name, cls in inspect.getmembers(mod):
            if not inspect.isclass(cls):
                continue
            # exclude any Dataset without a proper version (e.g. None).
            # This is a proxy for (abstract) base classes.
            if issubclass(cls, Dataset) and getattr(cls, 'version', None):
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


TYPE_REGISTRY = load_dataset_classes(biodb.data.datasets)
for name, cls in TYPE_REGISTRY.items():
    globals()[name] = cls


# Table registry should be reloaded every time from scratch since the state of
# DB might change throughout the course of execution
# NOTE in python 3.7+ we can just define __getattr__, cf. https://stackoverflow.com/a/48916205
def load_table_registry(latest_only=False):
    return [
        historical
        for historical in imported_datasets()
        if not latest_only or historical.is_latest()
    ]
