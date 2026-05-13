# terra-package

<img src="assets/logo.png" alt="Logo">

`terra-package` is a Python package for international trade analysis with
trade-network metrics, aggregated time-series tools, CES shock simulation and
TERRA API workflows.

## Installation

```bash
git clone https://github.com/istat-methodology/terra-package
cd terra-package
pip install -e .
```

## Requirements

- Python >= 3.8
- pandas >= 1.0
- networkx >= 2.0
- distinctiveness >= 0.1.5
- statsmodels
- matplotlib
- requests

## What It Does

- Computes network metrics from trade microdata.
- Uses precomputed network metrics when available.
- Analyzes aggregated trade time series.
- Simulates supplier-removal shocks with a CES model.
- Provides utilities for TERRA API payload classifications.

## Choose The Right Input Data

API and CSV are loading routes. A TERRA file saved locally and reloaded later
should be treated according to its data type.

| Data type | Load with | Compatible functions |
|---|---|---|
| Trade microdata | `TerraDataset` | `analyze_network()`, `analyze_basket()`, `simulate_shock()` |
| Precomputed network metrics | `NetworkMetricsDataset` | `analyze_network()` only |
| Aggregated time series | `TimeSeriesDataset` | `analyze_series()` only |

For detailed loading workflows, see [Data workflows](docs/data_workflows.md).
For TERRA API classification lookup tables, see
[API classifications](docs/api_classifications.md).

## Main Analysis Functions

The examples below use TERRA API workflows to highlight the package's
API-first usage. Local CSV loading is also supported and documented in
[Data workflows](docs/data_workflows.md). API examples require access to the
TERRA API.

### `analyze_network()`

Accepts `TerraDataset` or `NetworkMetricsDataset`. With `TerraDataset`, it
builds a trade network and computes node metrics. With `NetworkMetricsDataset`,
it uses precomputed metrics directly. When `base_period` is provided, it also
computes fixed-base indices.

```python
from terra_package import NetworkMetricsDataset, analyze_network

base_payload = {
    "percentage": "50",
    "transport": [0, 1, 2, 3, 4, 5, 7, 8, 9],
    "product": "TOT",
    "flow": 0,
    "weight": True,
    "position": None,
    "edges": None,
    "collapse": True,
}

metrics_ds = NetworkMetricsDataset.from_api(
    dataset="Intra",
    base_payload=base_payload,
    start_date="2025-05",
    end_date="2025-05",
    frequency="month",
)

metrics = analyze_network(metrics_ds, base_period="202505")
```

### `analyze_basket()`

Requires `TerraDataset`. It aggregates trade quantities over time for a
selected country, optionally filtered by partner, product and direction.

```python
from terra_package import TerraDataset, analyze_basket

trade_ds = TerraDataset.from_api_microdata(
    product_class="cpa",
    period="202505",
    country="IT",
    partner="ES",
    product="00",
    flow=1,
    criterion=2,
)

basket = analyze_basket(
    trade_ds,
    country="IT",
    partner="ES",
    product="00",
    direction="E",
)
```

### `analyze_series()`

Uses `TimeSeriesDataset` for aggregated time series. The backward-compatible
`TerraDataset` path is also supported for trade microdata aggregation. The
function computes moving averages, STL trends and an optional break model.

```python
from terra_package import TimeSeriesDataset, analyze_series

base_payload = {
    "flow": 1,
    "var": "00",
    "partner": "AC",
    "dataType": 2,
    "tipovar": 1,
    "varType": 1,
}

ts_ds = TimeSeriesDataset.from_api(
    base_payload=base_payload,
    countries=["IT"],
)

out = analyze_series(ts_ds, flow=1, break_date="2025-03")
```

### `simulate_shock()`

Requires `TerraDataset` with quantity and value data. It runs on one selected
period and simulates removal of one supplier using CES redistribution.

```python
from terra_package import TerraDataset, load_trade_microdata_from_api, simulate_shock

qty_ds = TerraDataset.from_api_microdata(
    product_class="cpa",
    period="202505",
    country="IT",
    partner=None,
    product="00",
    flow=1,
    criterion=2,
)

value_df = load_trade_microdata_from_api(
    product_class="cpa",
    period="202505",
    country="IT",
    partner=None,
    product="00",
    flow=1,
    criterion=1,
    cols_map={"qty": "VALUE_IN_EUROS", "value": []},
).rename(columns={"qty": "value"})

trade_df = qty_ds.data.merge(
    value_df[["source", "target", "period", "product", "flow", "value"]],
    on=["source", "target", "period", "product", "flow"],
)
trade_df = trade_df[(trade_df["qty"] > 0) & (trade_df["value"] > 0)]

trade_ds = TerraDataset.from_dataframe(
    trade_df,
    trade_to_network=True,
    mode="import",
    imp_exp=["1", "2"],
    two_values=True,
)

simulated = simulate_shock(
    trade_ds,
    country_from="ES",
    country_to="IT",
    period="202505",
    product="00",
    sigma=2,
)

simulated.simulation
```

## Examples

- [Graph API workflow](examples/graph_analysis_api.ipynb)
- [Aggregated time-series API workflow](examples/time_series_analysis_api.ipynb)
- [Precomputed network metrics example](examples/precomputed_network_metrics.py)
