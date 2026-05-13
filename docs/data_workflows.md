# Data Workflows

`terra-package` is organized around data types, not around whether data come
from an API or from a local file. API and CSV are loading routes. A TERRA file
saved locally must later be reloaded according to the data type it contains.

| Data type | Load with | Compatible functions |
|---|---|---|
| Trade microdata | `TerraDataset` | `analyze_network()`, `analyze_basket()`, `simulate_shock()` |
| Precomputed network metrics | `NetworkMetricsDataset` | `analyze_network()` only |
| Aggregated time series | `TimeSeriesDataset` | `analyze_series()` only |

Critical rules:

- Trade microdata, precomputed network metrics and aggregated time series are
  different data types.
- Precomputed network metrics work only with `analyze_network()`.
- Aggregated time series work only with `analyze_series()`.
- `simulate_shock()` requires trade microdata, value data for CES prices, and
  one selected `period`.
- `examples/time_series_analysis_api.ipynb` provides aggregated time-series
  data, not trade microdata.

## Trade Microdata

Trade microdata are raw trade-flow observations. Typical normalized columns are
`source`, `target`, `period`, `product`, `qty`, `value` and `flow`.

Trade microdata can come from:

- user-provided CSV files;
- the confirmed TERRA trade-microdata endpoint;
- a TERRA microdata file downloaded earlier and reloaded from CSV.

The confirmed endpoint for trade microdata is:

```text
https://api.terra.istat.it/graph/downloadData
```

Do not use `examples/time_series_analysis_api.ipynb` as a microdata source.
That notebook refers to aggregated time-series data.

### Load From The TERRA Microdata API

Use `TerraDataset.from_api_microdata()`:

```python
from terra_package import TerraDataset

trade_ds = TerraDataset.from_api_microdata(
    product_class="cpa",
    period="202505",
    country="IT",
    flow=1,
    criterion=2,
    partner="ES",
    product="00",
    transport=None,
)
```

Required parameters:

- `product_class`
- `period`
- `country`
- `flow`
- `criterion`

Optional parameters:

- `partner`: if omitted or `None`, requests all partners.
- `product`: if omitted or `None`, requests all products.
- `transport`: if omitted, `None` or `[]`, requests all transport types when
  supported by the endpoint.

The live endpoint returns one criterion per request: `criterion=1` returns
value fields such as `VALUE_IN_EUROS`, while `criterion=2` returns quantity
fields such as `QUANTITY_IN_KG`. Use data with both quantity and value when a
workflow, such as `simulate_shock()`, needs CES prices.

### Export And Reload Microdata

```python
trade_ds.to_csv("terra_microdata.csv")

reloaded = TerraDataset(
    "terra_microdata.csv",
    sep=",",
    encoding="utf-8",
    two_values=True,
)
```

The exported file already uses package-ready column names.

### Load Local CSV Microdata

Use `cols_map` when a CSV uses source-specific column names:

```python
from terra_package import TerraDataset

cols_map = {
    "source": "reporterISO",
    "target": "partnerISO",
    "period": "period",
    "product": "cmdCode",
    "qty": "qty",
    "flow": "flowDesc",
    "value": "primaryValue",
}

trade_ds = TerraDataset(
    "sample/com_trade_months.csv",
    sep=",",
    encoding="latin1",
    cols_map=cols_map,
    two_values=True,
)
```

Useful `TerraDataset` options:

- `cols_map`: maps source columns to package names.
- `sep` and `encoding`: handle CSV formats.
- `trade_to_network=True`: converts import/export rows into source-target
  links.
- `mode`: one of `"import"`, `"export"` or `"both"` when
  `trade_to_network=True`.
- `imp_exp`: labels used to identify import and export flows.
- `two_values=True`: preserves and validates both quantity and value.

## Precomputed Network Metrics

Precomputed network metrics are already-calculated node or country metrics.
They are compatible with `analyze_network()` only. Graph construction and
metric computation are skipped.

### Load From The TERRA Graph API

```python
from terra_package import NetworkMetricsDataset, analyze_network

base_payload = {
    "percentage": "50",
    "transport": [0, 1],
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
    start_date="2024-01",
    end_date="2024-03",
    frequency="month",
)

out = analyze_network(metrics_ds, base_period="202401")
```

### Load From CSV

```python
from terra_package import NetworkMetricsDataset, analyze_network

metrics_ds = NetworkMetricsDataset.from_csv(
    "sample/network_metrics.csv",
    sep=",",
    encoding="utf-8",
)

out = analyze_network(metrics_ds, base_period="202401")
```

Use `cols_map` if a CSV uses custom metric names.

## Aggregated Time Series

Aggregated time series are already summarized over time for a selected country,
partner, product and flow. They are compatible with `analyze_series()` only.

The reference API workflow is
[`../examples/time_series_analysis_api.ipynb`](../examples/time_series_analysis_api.ipynb).
It queries the TERRA time-series API, reads `diagMain`, and combines
country-level rows with fields such as `date`, `series` and `country`.

### Load From The TERRA Time-Series API

```python
from terra_package import TimeSeriesDataset, analyze_series

ts_ds = TimeSeriesDataset.from_api(
    base_payload={
        "flow": 1,
        "var": "00",
        "partner": "AC",
        "dataType": 2,
        "tipovar": 1,
        "varType": 1,
    },
    countries=["IT", "FR"],
)

out = analyze_series(ts_ds, flow=1, break_date="2025-03")
```

`TimeSeriesDataset.from_api()` preserves metadata from the request where
available, including country, partner, product, flow, data type and variable
type.

### Load From CSV

```python
from terra_package import TimeSeriesDataset, analyze_series

ts_ds = TimeSeriesDataset.from_csv(
    "time_series.csv",
    sep=",",
    encoding="utf-8",
    cols_map={
        "date": "Date",
        "series": "Value",
        "country": "Country",
        "partner": "Partner",
        "product": "Product",
        "flow": "Flow",
    },
)

out = analyze_series(ts_ds, flow=1, break_date="2025-03")
```

### Export And Reload Time Series

```python
ts_ds.to_csv("time_series.csv")
reloaded = TimeSeriesDataset.from_csv("time_series.csv")
```

CSV-loaded metadata code columns such as product codes are preserved as
strings, while analytical columns such as `series`, `value`, `qty` and
`unit_value` are converted to numeric values.
