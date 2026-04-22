import pandas as pd
import networkx as nx
from distinctiveness.dc import distinctiveness
import io
from contextlib import redirect_stdout

def calculate_node_metrics(G: nx.Graph, period: str) -> pd.DataFrame:
    """
    Compute a set of node-level network metrics for a given graph.

    The function computes weighted degree metrics, closeness and betweenness
    centrality (using inverse weights as distances), vulnerability, and
    distinctiveness centrality (D1). All edge weights are first normalized by
    the total weight of the graph.

    Parameters
    ----------
    G : networkx.Graph or networkx.DiGraph
        The trade network for a given period. If directed, both in-degree and
        out-degree are computed; otherwise these values are set to ``None``.
        The graph may contain a ``qty`` attribute on edges, which is
        normalized internally.
    period : str
        Period associated with the graph. This value is included in the
        returned DataFrame.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the following metrics for each node:

        - ``Period`` : The period identifier passed to the function.
        - ``Node`` : Node identifier.
        - ``Degree`` : Total weighted degree.
        - ``Out Degree`` : Weighted out-degree (directed graphs only).
        - ``In Degree`` : Weighted in-degree (directed graphs only).
        - ``Vulnerability`` : Defined as ``1 - in_degree`` when in-degree > 0,
          otherwise 0.
        - ``Closeness`` : Closeness centrality computed using inverse weight
          as distance.
        - ``Betweenness`` : Betweenness centrality computed using inverse
          weight as edge weight.
        - ``Distinctiveness`` : Distinctiveness centrality (D1), computed via
          the external ``distinctiveness`` package on the undirected version
          of the graph.

    Notes
    -----
    - All edge weights are normalized by dividing by the total sum of weights
      in the graph prior to computing any metric.
    - In directed graphs, the closeness and betweenness centrality take
      direction into account.
    - Distinctiveness centrality (D1) is always computed on an undirected
      version of the graph for stability.

    Raises
    ------
    ZeroDivisionError
        If the graph contains no weights or all weights are zero (division by
        zero when normalizing). This should be handled upstream by validating
        the dataset.
    """

    total_weight = sum(d.get("qty", 0) for _, _, d in G.edges(data=True))
    for _, _, d in G.edges(data=True):
        d["weight"] = (d.get("qty", 0) / total_weight) if total_weight > 0 else 0
    
    deg = dict(G.degree(weight="weight"))
    out_deg = dict(G.out_degree(weight="weight")) if G.is_directed() else {n: None for n in G.nodes()}
    in_deg = dict(G.in_degree(weight="weight")) if G.is_directed() else {n: None for n in G.nodes()}

    vulnerability = {}
    for k, v in in_deg.items():
        if v != 0:
            vulnerability[k] = 1 - v
        else:
            vulnerability[k] = 0
    
    inv_w = {(u, v): 1/d["weight"] if d.get("weight", 0) > 0 else 1e9999
                for u, v, d in G.edges(data=True)}
    nx.set_edge_attributes(G, inv_w, "inv_weight")
    clos = nx.closeness_centrality(G, distance="inv_weight")
    betw = nx.betweenness_centrality(G, weight="inv_weight")

    with redirect_stdout(io.StringIO()):  
        disti = distinctiveness(G.to_undirected(), alpha = 1, normalize = True, measures = ["D1"])["D1"]
    
    df_metrics = pd.DataFrame({
        "Period": period,
        "Node": list(G.nodes()),
        "Degree": [deg[n] for n in G.nodes()],
        "Out Degree": [out_deg[n] for n in G.nodes()],
        "In Degree": [in_deg[n] for n in G.nodes()],
        "Vulnerability": [vulnerability[n] for n in G.nodes()],
        "Closeness": [clos[n] for n in G.nodes()],
        "Betweenness": [betw[n] for n in G.nodes()],
        "Distinctiveness": [disti[n] for n in G.nodes()],
    })

    return df_metrics

def add_fixed_base_indices(full_metrics_df: pd.DataFrame, base_period=base_period) -> pd.DataFrame:
    """
    Compute fixed-base index numbers and a synthetic index for selected
    network metrics.
    """
    required_cols = {
        "Node", "Period", "Out Degree", "Betweenness", "Distinctiveness"
    }
    missing_cols = required_cols - set(full_metrics_df.columns)
    if missing_cols:
        raise ValueError(
            f"Missing required columns for index calculation: {sorted(missing_cols)}"
        )

    out = full_metrics_df.copy()

    metrics_map = {
        "Out Degree": "out_degree_index",
        "Betweenness": "betweenness_index",
        "Distinctiveness": "distinctiveness_index"
    }

    base_mask = out["Period"].astype(str) == str(base_period)

    if not base_mask.any():
        raise ValueError(
            f"Base period {base_period} not found in 'Period' column."
        )

    base_df = out.loc[
        base_mask,
        ["Node", *metrics_map.keys()]
    ].rename(
        columns={metric: f"{metric}_base" for metric in metrics_map}
    )

    out = out.merge(base_df, on="Node", how="left")

    for metric, index_name in metrics_map.items():
        base_col = f"{metric}_base"

        out[index_name] = (out[metric] / out[base_col]) * 100

        out.loc[
            out[base_col].isna() | (out[base_col] == 0),
            index_name
        ] = pd.NA

    out["synthetic_index"] = out[list(metrics_map.values())].mean(axis=1)

    out = out.drop(columns=[f"{metric}_base" for metric in metrics_map])

    return out
