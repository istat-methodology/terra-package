import pandas as pd
import networkx as nx
from .metrics import (calculate_node_metrics,
    add_fixed_base_indices,
    _prepare_series,
    _add_ma12,
    _add_stl_trend,
    _fit_break_model,
    _plot_series_break,
)
from .utils import TerraDataset

def analyze_network(df: TerraDataset, base_period=None) -> pd.DataFrame:
    """
    Compute network metrics for each node in a directed trade network across periods.

    The function converts each period of the input TerraDataset into a directed
    NetworkX graph and computes node-level metrics using `calculate_node_metrics`.
    Results from all periods are concatenated into a single DataFrame.

    If `base_period` is provided, the function also computes fixed-base indices
    for selected metrics and a synthetic index.

    Parameters
    ----------
    df : TerraDataset
        A validated TerraDataset object containing at least the
        columns ['source', 'target', 'period', 'product', 'qty'], and optionally 'flow' and 'value'.

    base_period : str or int, optional
        Period used as the base for fixed-base indices. For example, "202001".
        If None, only network metrics are computed.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing node metrics for each period, as returned
        by `calculate_node_metrics`. If `base_period`
        is specified, fixed-base indices and the synthetic index are also added.

    Raises
    ------
    TypeError
        If `df` is not an instance of TerraDataset.
    """
    if not isinstance(df, TerraDataset):
        raise TypeError("This function only accepts TerraDataset.")
    
    df = df.data
    all_metrics = []
    period = sorted(df['period'].unique())
    for p in period:
        df_p = df[df['period'] == p].copy()
        edge_attr = 'qty' if 'qty' in df_p.columns else None
        G_p = nx.from_pandas_edgelist(df_p, 'source', 'target',
                                        edge_attr=edge_attr, create_using=nx.DiGraph())
        metrics_df = calculate_node_metrics(G_p, p)

        all_metrics.append(metrics_df)
        print(f"Processed period: {p}")

    full_metrics_df = pd.concat(all_metrics, ignore_index=True)
    full_metrics_df = add_fixed_base_indices(
        full_metrics_df,
        base_period=base_period
    )
    
    return full_metrics_df

def analyze_basket(df: TerraDataset, country: str, partner:str = None, product: str = None, var: bool = False, direction: str = "E") -> pd.DataFrame:
    """
    Analyze the trade basket of a given country, optionally filtering by partner
    or product, and compute aggregated trade weights over time.

    The function extracts flows from a validated TerraDataset, selecting
    the specified country either as exporter ("E") or importer ("I"). It then
    optionally filters by trading partner and/or product. Trade weights are
    aggregated by period. If `var=True`, period-over-period variation is
    computed instead of absolute values.

    Parameters
    ----------
    df : TerraDataset
        A validated TerraDataset object.
    country : str
        Country used as source (exports) or target (imports), depending on
        the selected direction.
    partner : str, optional
        Partner country to filter by. Default is None.
    product : str, optional
        Product code to filter by. Default is None.
    var : bool, optional
        If True, compute period-over-period variation of aggregated weights.
        Default is False.
    direction : {'E', 'I'}, optional
        Trade direction: 'E' for exports (default), 'I' for imports. When 'I'
        is selected, source and target are swapped.

    Returns
    -------
    pd.DataFrame
        A DataFrame with:
        - ``period`` : trade period.
        - ``qty`` : aggregated weight or its relative variation if ``var=True``.

    Raises
    ------
    TypeError
        If `df` is not an instance of TerraDataset.
    ValueError
        If the selected filters return an empty dataset.
    """
    if not isinstance(df, TerraDataset):
        raise TypeError("This function only accepts TerraDataset.")
    
    if direction not in ["E", "I"]:
        raise ValueError("Direction must be 'E' for exports or 'I' for imports.")
    
    df = df.data
    if direction in ["I"]:
        df.loc[:, ['source', 'target']] = df[['target', 'source']].values
    df = df[df['source'] == country]
    if df.empty:
        raise ValueError(f"Country {country} in direction {direction} is not present in the dataset.")
    if product:
        df = df[df['product'] == product]
    if df.empty:
        raise ValueError(f"Product {product} in direction {direction} is not present in the dataset.")
    if partner:
        df = df[df['target'] == partner]
    if df.empty:
        raise ValueError(f"Partner {partner} in direction {direction} is not present in the dataset.")

    df = df.groupby(['period'], as_index=False)['qty'].sum()

    if var:
        df = df.groupby(['period'], as_index=False)["qty"].sum().sort_values(by=['period'], ascending=True)
        df["qty_lag"] = df["qty"].shift(1)
        df["qty"] = (df["qty"]-df["qty_lag"])/df["qty_lag"]
    return df[['period', 'qty']]

def analyze_series(
    df: "TerraDataset",
    country: str,
    partner: str = None,
    product: str = None,
    direction: str = "E",
    break_date: str = None,
    plot: bool = False,
    seasonal: int = 13,
    period: int = 12,
    nw_lags: int = 12,
    figsize: tuple = (14, 12)
) -> dict:
    """
    Analyze a trade time series for a given country, optionally filtering by
    partner or product. The function aggregates monthly quantity and value,
    computes implicit prices, 12-period moving averages, and STL trends.
    If `break_date` is provided, it also estimates a single-break model on
    each STL trend using Newey-West standard errors. If `plot=True`, it
    returns a 6-panel figure with raw series, moving averages, and STL trends.

    Parameters
    ----------
    df : TerraDataset
        A validated TerraDataset object.
    country : str
        Country used as source (exports) or target (imports), depending on
        the selected direction.
    partner : str, optional
        Partner country to filter by. Default is None.
    product : str, optional
        Product code to filter by. Default is None.
    direction : {'E', 'I'}, optional
        Trade direction: 'E' for exports (default), 'I' for imports.
    break_date : str, optional
        Break date used in the structural model. If None, the break model is
        not estimated. Default is None.
    plot : bool, optional
        If True, produce the 6-panel chart. Default is False.
    seasonal : int, optional
        Seasonal smoothing parameter for STL. Default is 13.
    period : int, optional
        Frequency used in STL decomposition. Default is 12.
    nw_lags : int, optional
        Number of lags used for Newey-West covariance estimation. Default is 12.
    figsize : tuple, optional
        Figure size for the chart. Default is (14, 12).

    Returns
    -------
    dict
        A dictionary with:
        - ``data`` : DataFrame with quantity, value, implicit price, MA(12),
          and STL trends;
        - ``results`` : coefficient tables for ``value``, ``qty``,
          and ``unit_value`` if ``break_date`` is provided, otherwise None;
        - ``models`` : fitted model objects if ``break_date`` is provided,
          otherwise None;
        - ``figure`` : matplotlib Figure if ``plot=True``, otherwise None;
        - ``axes`` : matplotlib Axes if ``plot=True``, otherwise None.

    Raises
    ------
    TypeError
        If `df` is not an instance of TerraDataset.
    ValueError
        If the selected filters return an empty dataset, if required columns are
        missing, or if the series are not suitable for log-STL analysis.
    """
    data = _prepare_series(
        df=df,
        country=country,
        partner=partner,
        product=product,
        direction=direction
    )

    data = _add_ma12(data, cols=["qty", "value", "unit_value"])
    data = _add_stl_trend(
        data=data,
        cols=["qty", "value", "unit_value"],
        period=period,
        seasonal=seasonal
    )

    models = None
    results = None

    if break_date is not None:
        models = {}
        results = {}

        for col in ["value", "qty", "unit_value"]:
            model, table = _fit_break_model(
                trend=data[f"{col}_trend"],
                dates=data["period"],
                break_date=break_date,
                nw_lags=nw_lags
            )
            models[col] = model
            results[col] = table

    fig, axes = None, None
    if plot:
        plot_break = break_date if break_date is not None else data["period"].iloc[0]

        prefix = f"{country}"
        if partner is not None:
            prefix += f"-{partner}"
        if product is not None:
            prefix += f" ({product}) — "
        else:
            prefix += " — "

        fig, axes = _plot_series_break(
            data=data,
            break_date=plot_break,
            title_prefix=prefix,
            figsize=figsize
        )

    return {
        "data": data,
        "results": results,
        "models": models,
        "figure": fig,
        "axes": axes
    }

def simulate_shock(df: TerraDataset, country_from: str, country_to: str, period:str, product: str = None, sigma: int = 5, eta: float = 1.5) -> TerraDataset:
    """
    Simulates a trade shock in which a supplier country (`country_from`) is removed 
    from the set of exporters to a target importing country (`country_to`). 
    The function computes how import shares and quantities adjust under a CES 
    demand system after the shock.
    The function computes three types of output quantities:
    1. CES model-driven quantities:
       internal quantities consistent with the CES model structure.
    2. Scale-adjusted quantities:
       CES quantities mapped back to the observed quantity scale.
    3. Constant aggregate quantity scenario:
       observed total import quantity is preserved and the removed
       supplier's flow is redistributed across remaining suppliers.
       
    Parameters
    ----------
    df : TerraDataset
        The dataset containing trade flows. Must be an instance of TerraDataset.
    country_from : str
        The country whose supply is removed (the shocked supplier).
    country_to : str
        The importer country affected by the shock.
    period : str
        Time period to analyze. Rows with matching period are selected.
    product : str, optional
        Product to filter for. If None, the function aggregates over products.
    sigma : int, optional
        Elasticity of substitution across supplier countries in the CES demand system.
        It determines how easily imports can be reallocated across suppliers in response 
        to relative price changes. Higher values imply greater substitutability between 
        suppliers. Default is 5.
    eta : float, optional
        Price elasticity of total import demand. Governs how total expenditure 
        adjusts in response to changes in the CES price index. Higher values 
        imply a stronger demand response to price changes. Default is 1.5.

    Returns
    -------
    TerraDataset
        The updated TerraDataset object with a `simulation` attribute containing 
        the results of the shock simulation, including:
        - price
        - alpha (CES preference weights)
        - share_base, share_post (pre- and post-shock import shares)
        CES MODEL-DRIVEN QUANTITIES
        - q_base, q_new (quantities before and after the shock) 
        - q_delta (change in quantities)
        SCALE-ADJUSTED QUANTITIES
        - qty_new_scale_adjusted, qty_delta_scale_adjusted 
        CONSTANT AGGREGATE QUANTITY SCENARIO
        - qty_new_constant_total, qty_delta_constant_total

    Raises
    ------
    TypeError
        If the input `df` is not an instance of TerraDataset.
    ValueError
        If the selected period is not present.
        If filtering by product results in an empty dataset.
        If the shock is not applicable (i.e., the shocked country is the sole supplier).
    """

    if not isinstance(df, TerraDataset):
        raise TypeError("This function only accepts TerraDataset.")
    
    data = df.data.copy()
    data = data[data["period"] == period]

    if data.empty:
        raise ValueError(f"Period {period} is not present in the dataset.")

    # product filter
    if product:
        data = data[data["product"] == product]
        if data.empty:
            raise ValueError(f"No data found for product {product}.")
    else:
        data = data.groupby(
            ["source", "target"], as_index=False
        )[["qty", "value"]].sum()

    # subset importer
    df_shock = data[data.target == country_to].copy()

    if df_shock[df_shock.source != country_from].empty:
        raise ValueError(
            f"Simulation not applicable: {country_from} is the only supplier."
        )

    # prices
    df_shock["price"] = df_shock["value"] / df_shock["qty"]

    # initial expenditure
    E = df_shock["value"].sum()

    # 🔹 observed shares
    df_shock["share_base"] = df_shock["value"] / E

    # 🔹 alpha calibration (CORRECT)
    df_shock["alpha"] = df_shock["share_base"] / (
        df_shock["price"] ** (1 - sigma)
    )
    df_shock["alpha"] = df_shock["alpha"] / df_shock["alpha"].sum()

    # 🔹 initial price index
    P = (df_shock["alpha"] * df_shock["price"] ** (1 - sigma)).sum() ** (
        1 / (1 - sigma)
    )

    # 🔹 baseline quantities
    df_shock["q_base"] = df_shock["share_base"] * E / df_shock["price"]

    # =========================
    # 🔥 SHOCK
    # =========================
    df_shock.loc[df_shock.source == country_from, "alpha"] = 0

    if df_shock["alpha"].sum() == 0:
        raise ValueError("All suppliers removed.")

    # 🔹 rinormalizzazione alpha
    df_shock["alpha"] = df_shock["alpha"] / df_shock["alpha"].sum()

    # 🔹 new quotes
    df_shock["weight"] = df_shock["alpha"] * df_shock["price"] ** (1 - sigma)
    df_shock["share_post"] = df_shock["weight"] / df_shock["weight"].sum()

    # 🔹 new price index
    P_new = (df_shock["alpha"] * df_shock["price"] ** (1 - sigma)).sum() ** (
        1 / (1 - sigma)
    )

    # 🔥 ELASTIC DEMAND (KEY DIFFERENCE)
    E_new = E * (P_new / P) ** (1 - eta)

    # =====================================================
    # 1) CES MODEL-DRIVEN QUANTITIES
    # =====================================================
    # 🔹 new quantities
    df_shock["q_new"] = df_shock.apply(
        lambda row: (
            row["share_post"] * E_new / row["price"]
            if row["price"] != 0 else 0
        ),
        axis=1
    )

    df_shock["q_delta"] = df_shock["q_new"] - df_shock["q_base"]

    # =====================================================
    # 2) SCALE-ADJUSTED QUANTITIES
    # =====================================================
    
    df_shock["scale_factor_qty"] = df_shock.apply(
        lambda row: (
            row["qty"] / row["q_base"]
            if row["q_base"] != 0 else 0
        ),
        axis=1
    )
    
    df_shock["qty_new_scale_adjusted"] = (
        df_shock["q_new"] * df_shock["scale_factor_qty"]
    )
    
    df_shock["qty_delta_scale_adjusted"] = (
        df_shock["qty_new_scale_adjusted"] - df_shock["qty"]
    )
    
    # =====================================================
    # 3) CONSTANT AGGREGATE QUANTITY SCENARIO
    # =====================================================
    
    removed_qty = df_shock.loc[
        df_shock["source"] == country_from,
        "qty"
    ].sum()
    
    df_shock["qty_redistributed_from_removed"] = (
        df_shock["share_post"] * removed_qty
    )
    
    df_shock["qty_new_constant_total"] = df_shock.apply(
        lambda row: (
            0
            if row["source"] == country_from
            else row["qty"] + row["qty_redistributed_from_removed"]
        ),
        axis=1
    )
    
    df_shock["qty_delta_constant_total"] = (
        df_shock["qty_new_constant_total"] - df_shock["qty"]
    )
    
    # =====================================================
    # METADATA
    # =====================================================
    
    df_shock["period"] = str(period)
    df_shock["product"] = str(product) if product else "all"
    
    # =====================================================
    # FINAL OUTPUT
    # =====================================================
    
    df.simulation = df_shock[
        [
            "source", "target", "period", "product",
            "qty", "value", "price",
    
            "alpha",
            "share_base", "share_post",
    
            # 1) CES quantities (original names)
            "q_base",
            "q_new",
            "q_delta",
    
            # 2) Scale-adjusted
            "scale_factor_qty",
            "qty_new_scale_adjusted",
            "qty_delta_scale_adjusted",
    
            # 3) Constant total quantity
            "qty_redistributed_from_removed",
            "qty_new_constant_total",
            "qty_delta_constant_total"
        ]
    ]

    return df
