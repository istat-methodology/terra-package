import numpy as np
import pandas as pd
import networkx as nx
from distinctiveness.dc import distinctiveness
import io
from contextlib import redirect_stdout
import matplotlib.pyplot as plt
from statsmodels.tsa.seasonal import STL
import statsmodels.formula.api as smf

### NETWORK ANALYSIS
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

### TIME SERIES ANALYSIS
def _parse_period(period_series: pd.Series) -> pd.Series:
    """
    Parse 'period' in YYYYMM format into pandas datetime.
    """
    s = period_series.astype(str).str.strip()

    if not s.str.fullmatch(r"\d{6}").all():
        invalid = s[~s.str.fullmatch(r"\d{6}")].unique()[:5]
        raise ValueError(f"'period' must be in YYYYMM format. Invalid examples: {invalid}")

    return pd.to_datetime(s, format="%Y%m", errors="coerce")

def _prepare_series(
    df: "TerraDataset",
    country: str,
    partner: str = None,
    product: str = None,
    direction: str = "E"
) -> pd.DataFrame:
    """
    Prepare monthly aggregated trade series.
    """
    if not isinstance(df, TerraDataset):
        raise TypeError("This function only accepts TerraDataset.")

    if direction not in ["E", "I"]:
        raise ValueError("Direction must be 'E' for exports or 'I' for imports.")

    data = df.data.copy()

    required_cols = {"source", "target", "period", "product", "qty", "value"}
    missing_cols = required_cols - set(data.columns)
    if missing_cols:
        raise ValueError(
            f"This function requires columns {sorted(required_cols)}. "
            f"Missing: {sorted(missing_cols)}. Use TerraDataset(..., two_values=True)."
        )

    if direction == "I":
        data.loc[:, ["source", "target"]] = data[["target", "source"]].values

    data = data[data["source"] == country]
    if data.empty:
        raise ValueError(
            f"Country {country} in direction {direction} is not present in the dataset."
        )

    if product is not None:
        data = data[data["product"] == product]
        if data.empty:
            raise ValueError(
                f"Product {product} in direction {direction} is not present in the dataset."
            )

    if partner is not None:
        data = data[data["target"] == partner]
        if data.empty:
            raise ValueError(
                f"Partner {partner} in direction {direction} is not present in the dataset."
            )

    data = (
        data.groupby("period", as_index=False)
        .agg({"qty": "sum", "value": "sum"})
        .sort_values("period")
        .reset_index(drop=True)
    )

    data = data[data["qty"] > 0].copy()
    if data.empty:
        raise ValueError("No positive aggregated quantities are available after filtering.")

    data["unit_value"] = data["value"] / data["qty"]
    data["period"] = _parse_period(data["period"])

    return (
        data[["period", "qty", "value", "unit_value"]]
        .sort_values("period")
        .reset_index(drop=True)
    )

def _add_ma12(data: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Add 12-period moving averages to selected columns.
    """
    out = data.copy()
    for col in cols:
        out[f"{col}_ma12"] = out[col].rolling(window=12, min_periods=1).mean()
    return out

def _add_stl_trend(
    data: pd.DataFrame,
    cols: list[str],
    period: int = 12,
    seasonal: int = 13
) -> pd.DataFrame:
    """
    Add STL trend columns computed on log-transformed series.
    """
    out = data.copy()

    for col in cols:
        if (out[col] <= 0).any():
            raise ValueError(
                f"Column '{col}' contains non-positive values; log-STL cannot be computed."
            )

        stl = STL(
            np.log(out[col]),
            period=period,
            seasonal=seasonal,
            robust=True
        )
        res = stl.fit()
        out[f"{col}_trend"] = res.trend

    return out

def _break_design_data(
    dates: pd.Series,
    trend: pd.Series,
    break_date
) -> pd.DataFrame:
    """
    Build the regressors for a single-break model.
    """
    break_date = pd.to_datetime(break_date)

    if break_date < dates.min() or break_date > dates.max():
        raise ValueError(
            f"break_date {break_date.date()} is outside sample range "
            f"[{dates.min().date()}, {dates.max().date()}]."
        )

    t = np.arange(len(dates), dtype=int)
    break_idx = np.where(dates >= break_date)[0]

    if len(break_idx) == 0:
        raise ValueError("No observations at or after break_date.")

    t_break = int(break_idx[0])
    d_break = (t >= t_break).astype(int)
    post_break = np.clip(t - t_break, a_min=0, a_max=None)

    return pd.DataFrame({
        "trend": trend.values,
        "t": t,
        "D_break": d_break,
        "post_break": post_break
    })

def _fit_break_model(
    trend: pd.Series,
    dates: pd.Series,
    break_date,
    nw_lags: int = 12
):
    """
    Fit a single-break model with Newey-West standard errors.
    """
    reg_data = _break_design_data(
        dates=dates,
        trend=trend,
        break_date=break_date
    )

    result = smf.ols(
        formula="trend ~ t + D_break + post_break",
        data=reg_data
    ).fit(
        cov_type="HAC",
        cov_kwds={"maxlags": nw_lags}
    )

    ci = result.conf_int()

    table = pd.DataFrame({
        "term": result.params.index,
        "estimate": result.params.values,
        "std_error": result.bse.values,
        "t_value": result.tvalues.values,
        "p_value": result.pvalues.values,
        "conf_low": ci.iloc[:, 0].values,
        "conf_high": ci.iloc[:, 1].values
    })

    table["pct_effect"] = np.nan
    mask = table["term"] == "D_break"
    table.loc[mask, "pct_effect"] = 100 * (np.exp(table.loc[mask, "estimate"]) - 1)

    return result, table

def _plot_series_break(
    data: pd.DataFrame,
    break_date,
    title_prefix: str = "",
    figsize: tuple = (14, 12)
):
    """
    Plot raw series, MA(12), and STL trends.
    """
    break_date = pd.to_datetime(break_date)

    fig, axes = plt.subplots(3, 2, figsize=figsize, sharex=True)
    fig.subplots_adjust(hspace=0.35, wspace=0.20)

    specs = [
        ("value", "Values", "Euro"),
        ("qty", "Quantity", "Kg"),
        ("unit_value", "Implicit price", "Euro/Kg")
    ]

    for i, (col, label, ylab) in enumerate(specs):
        axes[i, 0].plot(data["period"], data[col], label="Raw", alpha=0.7)
        axes[i, 0].plot(data["period"], data[f"{col}_ma12"], label="MA(12)")
        axes[i, 0].axvline(break_date, linestyle="--")
        axes[i, 0].set_title(f"{title_prefix}{label} (raw + MA12)")
        axes[i, 0].set_ylabel(ylab)
        axes[i, 0].legend()
        axes[i, 0].grid(True, alpha=0.3)

        axes[i, 1].plot(data["period"], data[f"{col}_trend"])
        axes[i, 1].axvline(break_date, linestyle="--")
        axes[i, 1].set_title(f"{title_prefix}{label} trend (log-STL)")
        axes[i, 1].set_ylabel("Trend")
        axes[i, 1].grid(True, alpha=0.3)

    axes[-1, 0].set_xlabel("Year")
    axes[-1, 1].set_xlabel("Year")

    return fig, axes
