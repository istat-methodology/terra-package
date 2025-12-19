import pandas as pd
import networkx as nx
from .metrics import calculate_node_metrics
from .utils import TerraDataset

def analyze_network(df: TerraDataset) -> pd.DataFrame:
    """
    Compute network metrics for each node in a directed trade network across periods.

    The function converts each period of the input TerraDataset into a directed
    NetworkX graph and computes node-level metrics using `calculate_node_metrics`.
    Results from all periods are concatenated into a single DataFrame.

    Parameters
    ----------
    df : TerraDataset
        A validated TerraDataset object containing at least the
        columns ['source', 'target', 'period'], and optionally 'weight'.
    
    Returns
    -------
    pd.DataFrame
        A DataFrame containing node metrics for each period, as returned
        by `calculate_node_metrics`.

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
        df_p = df[df['period'] == p]
        edge_attr = 'weight' if 'weight' in df_p.columns else None
        G_p = nx.from_pandas_edgelist(df_p, 'source', 'target',
                                        edge_attr=edge_attr, create_using=nx.DiGraph())
        metrics_df = calculate_node_metrics(G_p, p)
        all_metrics.append(metrics_df)

    full_metrics_df = pd.concat(all_metrics, ignore_index=True)
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
        - ``weight`` : aggregated weight or its relative variation if ``var=True``.

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
    
    df = df.groupby(['period'], as_index=False)['weight'].sum()
    
    if var:
        df = df.groupby(['period'], as_index=False)["weight"].sum().sort_values(by=['period'], ascending=True)
        df["weight_lag"] = df["weight"].shift(1)
        df["weight"] = (df["weight"]-df["weight_lag"])/df["weight_lag"]
    return df[['period', 'weight']]

def simulate_shock(df: TerraDataset, country_from: str, country_to: str, period:str, product: str = None, sigma: int = 5) -> TerraDataset:
    """
    Simulates a trade shock in which a supplier country (`country_from`) is removed 
    from the set of exporters to a target importing country (`country_to`). 
    The function computes how import shares and quantities adjust under a CES 
    demand system after the shock.

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
        Elasticity of substitution in the CES demand system. Default is 5.

    Returns
    -------
    TerraDataset
        The updated TerraDataset object with a `simulation` attribute containing 
        the results of the shock simulation, including:
        - price
        - alpha (CES preference weights)
        - share_base, share_post (pre- and post-shock import shares)
        - q_base, q_new (quantities before and after the shock)
        - q_delta (change in quantities)

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
    
    if product:
        data = data[data["product"] == product]
        if df.empty:
            raise ValueError(f"No data found for product {product} trade by {country_from}.")
    else:
        data.groupby(["source","target","product"], as_index=False)[["value", "value2"]].sum()

    if data[(data.source != country_from) & (data.target == country_to)].empty:
        raise ValueError(f"Simulation not applicable, since there is only {country_from} as supplier for {country_to}.")
    
    df_shock = data[data.target == country_to].copy()
    df_shock["price"] = df_shock["value"] / df_shock["value2"]
    df_shock["alpha"] = df_shock["value2"] * df_shock["price"]**(sigma - 1)
    df_shock["alpha"] = df_shock["alpha"] / df_shock["alpha"].sum()

    Q_tot = df_shock["value2"].sum()
    df_shock["weight"] = df_shock["alpha"] * df_shock["price"]**(1 - sigma)
    df_shock["share_base"] = df_shock["weight"] / df_shock["weight"].sum()
    P = (df_shock["alpha"] * df_shock["price"]**(1 - sigma)).sum()**(1 / (1 - sigma))
    E = P * Q_tot
    df_shock["q_base"] = df_shock["share_base"] * E / df_shock["price"]
    
    df_shock.loc[df_shock.source == country_from, "alpha"] = 0

    df_shock["weight"] = df_shock["alpha"] * df_shock["price"]**(1 - sigma)
    if df_shock["weight"].sum() != 0:
        df_shock["share_post"] = df_shock["weight"] / df_shock["weight"].sum()
    else:
        df_shock["share_post"] = 0

    P_new = (df_shock["alpha"] * df_shock["price"]**(1 - sigma)).sum()**(1 / (1 - sigma))
    E_new = P_new * Q_tot
    # Add a check to avoid division by zero if Prezzo is zero
    df_shock["q_new"] = df_shock.apply(lambda row: row["share_post"] * E_new / row["price"] if row["price"] != 0 else 0, axis=1)
    df_shock["q_delta"] = df_shock["q_new"] - df_shock["q_base"]
    df.simulation = df_shock[["source", "target", "period", "product", "value", "value2", "price", "alpha", "share_base", "share_post", "q_base", "q_new", "q_delta"]]
    return df