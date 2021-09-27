import fnmatch
import networkx as nx

from .db import ImportedTable
from .registry import TYPE_REGISTRY


def glob_datasets(glob, tables_only=False):
    classes = []
    for cls in TYPE_REGISTRY.values():
        if tables_only and not issubclass(cls, ImportedTable):
            continue

        if fnmatch.fnmatch(cls.__name__, glob):
            classes.append(cls)

    return classes


def build_code_dag(tables_only=True):
    """Build the dataset DAG as per current state of code, ie registry."""
    G = nx.DiGraph()

    def dataset_has_type_of_interest(dataset):
        return not tables_only or issubclass(dataset, ImportedTable)

    for dataset in TYPE_REGISTRY.values():
        if not dataset_has_type_of_interest(dataset):
            continue

        G.add_node(dataset.__name__)

        for dep in dataset.depends:
            if not dataset_has_type_of_interest(dep):
                continue

            G.add_node(dep.__name__)
            G.add_edge(dep.__name__, dataset.__name__)

    return G


def draw_code_dag(path, tables_only=True, nodes_of_interest_glob=None):
    """Draws the dataset DAG as per current state of code, ie registry.

    This requires: graphviz and libgraphiz-dev (apt) and pygraphviz (pip).
    """
    G = build_code_dag(tables_only=tables_only)

    if nodes_of_interest_glob:
        nodes_of_interest = set(sum([
            [cls.__name__ for cls in glob_datasets(node)]
            for node in nodes_of_interest_glob
        ], []))

        descendants = set().union(*[
            nx.algorithms.dag.descendants(G, node)
            for node in nodes_of_interest
        ])
        ancestors = set().union(*[
            nx.algorithms.dag.ancestors(G, node)
            for node in nodes_of_interest
        ])

        to_keep = set().union(nodes_of_interest, ancestors, descendants)
        nodes = list(G.nodes().keys())

        for node in nodes:
            if node not in to_keep:
                G.remove_node(node)

        nx.set_node_attributes(G, {
            node: '#0c97ae' if node in nodes_of_interest else '#dddddd'
            for node in G.nodes()
        }, 'color')

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
    P.draw(path)
