#!/usr/bin/env python3
import json
import sys
from pathlib import Path

# add repo root to $PYTHONPATH
sys.path.append(str(Path(__file__).absolute().parent.parent.parent))

from biodb.data.plot import plot_dag
from biodb.data.db import init_db


def status():
    print('--> imported tables:')
    for _, hdataset in sorted(load_table_registry().items()):
        is_latest = hdataset.is_latest()
        print('* %s %s' % (hdataset.name.ljust(60), 'latest' if is_latest else 'stale'))


if __name__ == '__main__':
    init_db()

    from biodb.data.registry import ClinVarVCFTable
    from biodb.data.registry import load_table_registry

    status()

    clinvar = ClinVarVCFTable()
    # print(json.dumps(clinvar.formula, indent=4))
    # plot_dag([ClinVarVCFTable], 'dag.svg') # needs networkx and pygraphviz
    clinvar.produce_recursive()

    status()
