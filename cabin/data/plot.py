# FIXME currently won't work because it needs networkx and pygraphviz, see
# requirements.txt of prototype repo: https://gitlab.com/streamlinegenomics/biodb_prototypes_2020
# add dependencies and make this alive if it seems worth it, e.g. plot the
# entire graph in CI/CD and store as artifact?
import json
import networkx as nx


def build_dag(datasets):
    G = nx.DiGraph()
    nodes = datasets.copy()
    while nodes:
        current = nodes.pop()
        G.add_node(current.name,
                   dataset=current,
                   tooltip=json.dumps(current.formula, indent=4),
                   **pygraphviz_kw(current))
        for key, inp in current.inputs.items():
            G.add_edge(inp.name, current.name)
            nodes.append(inp)

    return G


def pygraphviz_kw(dataset):
    kw = {}
    kw['style'] = 'filled'
    kw['label'] = dataset.name
    if dataset.is_root:
        kw['fillcolor'] = '#d2f8d2'
    else:
        kw['fillcolor'] = '#ffffff'
    kw['shape'] = 'rectangle'
    return kw


def plot_dag(datasets, path):
    G = build_dag(datasets)
    P = nx.drawing.nx_agraph.to_agraph(G)

    # attributes reference: https://www.graphviz.org/doc/info/attrs.html
    P.graph_attr['fontname'] = 'monospace'
    P.node_attr['fontname'] = 'monospace'
    P.edge_attr['fontname'] = 'monospace'

    P.node_attr['fontsize'] = 10
    P.edge_attr['fontsize'] = 8

    P.graph_attr['margin'] = .5
    P.node_attr['margin'] = .05

    P.graph_attr['rankdir'] = 'LR'
    P.graph_attr['ranksep'] = .2
    P.graph_attr['nodesep'] = .8

    P.edge_attr['lblstyle'] = 'sloped'
    P.edge_attr['arrowhead'] = 'vee'
    P.layout(prog='dot')
    P.draw(path)

    print('--> Dataset Graph saved to ' + path)
