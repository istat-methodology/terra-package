# terra-package Internal Workflow Map

Structured internal reference for supported data types, loaders, API routes,
analytical functions, outputs and control checks.

## 1. Input Data Types

| Input data type | Represents | TERRA API | User local CSV | Object | Compatible functions |
|---|---|---|---|---|---|
| Trade microdata | Raw trade-flow observations: source, target, period, product, flow, plus qty, value or both | Yes: `graph/downloadData` | Yes | `TerraDataset` | `analyze_network()`, `analyze_basket()`, `simulate_shock()` |
| Precomputed network metrics | Already-calculated node/country metrics such as out-degree, betweenness and distinctiveness | Yes: TERRA Graph API | Yes | `NetworkMetricsDataset` | `analyze_network()` only |
| Aggregated time series | Already-aggregated time observations for selected country/partner/product/flow | Yes: `time-series/ts` | Yes | `TimeSeriesDataset` | `analyze_series()` only |
| Reference classifications | Lookup tables for valid API payload values | Yes: `/cls` endpoints | Can be saved to CSV | `pandas.DataFrame` | No analytical functions |

| Object passed | `analyze_network()` | `analyze_basket()` | `analyze_series()` | `simulate_shock()` |
|---|---|---|---|---|
| `TerraDataset` | Accepted | Accepted | Accepted via backward-compatible trade aggregation path | Accepted; requires qty and value |
| `NetworkMetricsDataset` | Accepted; skips graph construction | Rejected: only `TerraDataset` accepted | Rejected: precomputed metrics cannot be used for time-series analysis | Rejected: cannot be used for CES simulation |
| `TimeSeriesDataset` | Rejected: use `TerraDataset` or `NetworkMetricsDataset` | Rejected: requires trade microdata | Accepted | Rejected: requires trade microdata |
| Classification `DataFrame` | Rejected unless wrapped as the correct object | Rejected | Rejected | Rejected |

## 2. Loading Workflows

### `TerraDataset`

Purpose: trade microdata, from local CSV, in-memory DataFrame or
`graph/downloadData` API response.

| Loader | Required parameters | Optional/default parameters | Expected input | Output | Notes |
|---|---|---|---|---|---|
| `TerraDataset(...)` | `path` | `trade_to_network=False`; `mode="both"`; `imp_exp=None` -> `["I", "E"]`; `two_values=False`; `cols_map=None`; `sep=","`; `encoding="utf-8"` | CSV with source, target, period, product; flow if `trade_to_network=True`; at least one of qty/value; both if `two_values=True` | `TerraDataset` | Validates duplicate edges and numeric measure columns |
| `from_dataframe()` | `data` | Same dataset options as CSV constructor | In-memory DataFrame with trade microdata columns | `TerraDataset` | Same validation as CSV path |
| `from_api_microdata()` | `product_class`, `period`, `country`, `flow`, `criterion` | `partner=None`; `product=None`; `transport=None`; `method="post"`; `timeout=30`; `api_cols_map=None`; `request_session=None`; plus `TerraDataset` options | `graph/downloadData` response normalized to package schema | `TerraDataset` | Use `two_values=True` when both qty and value are required, for example CES |
| `to_csv()` | `path` | `sep=","`; `encoding="utf-8"`; `index=False` | Normalized `TerraDataset.data` | CSV file | Reload later with `TerraDataset(...)` |

Base required columns are `source`, `target`, `period` and `product`.
At least one analytical measure, `qty` or `value`, must be present.
`two_values=True` requires both `qty` and `value`.
`trade_to_network=True` additionally requires `flow` and applies import/export
source-target conversion.

### `NetworkMetricsDataset`

| Loader | Required parameters | Optional/default parameters | Expected input | Output | Notes |
|---|---|---|---|---|---|
| `NetworkMetricsDataset(data)` | `data` | `cols_map=None` | DataFrame containing precomputed metrics | `NetworkMetricsDataset` | For already-calculated metrics only |
| `from_csv()` | `path` | `sep=","`; `encoding="utf-8"`; `cols_map=None`; `**read_csv_kwargs` | CSV with `Node`, `Period`, `Out Degree`, `Betweenness`, `Distinctiveness` or mappable equivalents | `NetworkMetricsDataset` | `Period` accepts `YYYYMM` or `YYYY-MM` |
| `from_api()` | `dataset`, `base_payload` | `start_date=None`; `end_date=None`; `frequency="month"`; `sleep_seconds=0.5`; `verbose=True`; `request_session=None`; `endpoint=None` | TERRA Graph API metrics response under `metrics` field | `NetworkMetricsDataset` | If dates are omitted, metadata endpoint supplies graph range |

Required normalized metric columns: `Node`, `Period`, `Out Degree`,
`Betweenness` and `Distinctiveness`.
Optional metrics include `Density`, `Degree`, `In Degree`, `Vulnerability` and
`Closeness`.
No dedicated `to_csv()` method exists; export with
`metrics_ds.data.to_csv(...)`.

### `TimeSeriesDataset`

| Loader | Required parameters | Optional/default parameters | Expected input | Output | Notes |
|---|---|---|---|---|---|
| `TimeSeriesDataset(data)` | `data` | `cols_map=None`; `allow_duplicates=False`; `preserve_metadata_strings=False` | DataFrame with date or period and at least one numeric series column | `TimeSeriesDataset` | Normalizes date and period |
| `from_dataframe()` | `data` | `cols_map=None`; `allow_duplicates=False` | In-memory aggregated time series | `TimeSeriesDataset` | Extra metadata columns are kept |
| `from_csv()` | `path` | `sep=","`; `encoding="utf-8"`; `cols_map=None`; `allow_duplicates=False`; `**read_csv_kwargs` | CSV with date/period plus `series`, `value`, `qty` or `unit_value` | `TimeSeriesDataset` | Reads as strings, preserves metadata code strings, converts numeric series |
| `from_api()` | `country` or `countries` | `base_payload=None`; `endpoint=None`; `response_path="diagMain"`; `timeout=30`; `sleep_seconds=0`; `verbose=False`; `**payload_kwargs` | `time-series/ts` response records, `diagMain` by default | `TimeSeriesDataset` | Posts `base_payload` plus one country at a time |
| `to_csv()` | `path` | `sep=","`; `encoding="utf-8"`; `index=False` | Normalized `TimeSeriesDataset.data` | CSV file | Reload with `TimeSeriesDataset.from_csv()` |

Normalized schema always includes `date` and `period`.
Numeric series columns can be `series`, `value`, `qty` or `unit_value`.
Metadata columns include `country`, `partner`, `product`, `flow`, `data_type`
and `tipovar` when available.
Duplicate time observations are checked by period plus canonical series key
unless `allow_duplicates=True`.

## 3. Analytical Functions

| Function | Accepted inputs | Rejected inputs | Required parameters | Optional/default parameters | Output |
|---|---|---|---|---|---|
| `analyze_network()` | `TerraDataset`; `NetworkMetricsDataset` | `TimeSeriesDataset`; raw classifications | `df` | `base_period=None`; `verbose=False` | DataFrame with `Node`, `Period`, metrics and optional fixed-base indices |
| `analyze_basket()` | `TerraDataset` | `TimeSeriesDataset`; `NetworkMetricsDataset`; classifications | `df`, `country` | `partner=None`; `product=None`; `var=False`; `direction="E"`; `measure="qty"` | DataFrame with `period` and selected measure column |
| `analyze_series()` | `TimeSeriesDataset`; `TerraDataset` | `NetworkMetricsDataset`; classifications | `df`; `country` required for `TerraDataset` | `country=None`; `partner=None`; `product=None`; `direction="E"`; `flow=None`; `break_date=None`; `plot=False`; `seasonal=13`; `period=12`; `nw_lags=12`; `figsize=(14, 12)` | Dict: `data`, `results`, `models`, `figure`, `axes` |
| `simulate_shock()` | `TerraDataset` with qty and value | `NetworkMetricsDataset`; `TimeSeriesDataset`; classifications | `df`, `country_from`, `country_to`, `period` | `product=None`; `sigma=5`; `eta=1.5` | `TerraDataset` with `.simulation` DataFrame |

`analyze_network()` builds graphs from trade microdata or reuses precomputed
metrics.
`analyze_basket()` aggregates one selected measure over time: `qty` or `value`.
`analyze_series()` computes moving averages, STL trends and optional break
models.
`simulate_shock()` simulates supplier removal with CES redistribution and
requires both `qty` and `value`.

## 4. API Workflows

### Trade Microdata: `graph/downloadData`

Endpoint:

```text
https://api.terra.istat.it/graph/downloadData
```

Required parameters:

- `product_class`
- `period`
- `country`
- `flow`
- `criterion`

Optional parameters:

- `partner`
- `product`
- `transport`

`criterion=0`: value plus quantity when both are returned by the selected
payload.
`criterion=1`: value only, supports `analyze_basket(measure="value")` when
value is returned.
`criterion=2`: quantity only, supports `analyze_basket(measure="qty")` when
qty is returned.

Users should verify returned fields. Availability depends on the actual API
response, not only on `product_class`.

| API field | Normalized field |
|---|---|
| `DECLARANT_ISO` | `source` |
| `PARTNER_ISO` | `target` |
| `PERIOD` | `period` |
| `PRODUCT` | `product` |
| `FLOW` | `flow` |
| `QUANTITY_IN_KG` | `qty` |
| `VALUE_IN_EUROS` | `value` |

### Graph API Precomputed Metrics

Endpoint pattern:

```text
https://api.terra.istat.it/graph/graph{dataset}{Month|Trim}
```

Metadata endpoint:

```text
https://api.terra.istat.it/cls/metadata
```

Loader: `NetworkMetricsDataset.from_api()`.
Posts `base_payload` plus `period` and reads the `metrics` response field.

### Time-Series API

Endpoint:

```text
https://api.terra.istat.it/time-series/ts
```

Loader: `TimeSeriesDataset.from_api()`.
Posts `base_payload` plus one country at a time.
Reads `diagMain` by default and normalizes date, period, series columns and
metadata.

### Reference Classifications

| Name | Endpoint suffix |
|---|---|
| `products_extra` | `productsExtra` |
| `products_intra` | `productsIntra` |
| `products_cpa` | `productsCPA` |
| `transports` | `transports` |

Use `get_terra_classification(name, lang="en", save_path=None)` for one table.
Use `download_terra_classifications(output_dir, classifications=None,
lang="en")` for multiple tables.
Classification tables are lookup data, not analytical datasets.

## 5. Output Map

| Workflow | Input | Loader | Function | Main parameters | Output type | Output columns/keys | Notes |
|---|---|---|---|---|---|---|---|
| Build network from microdata | Trade microdata | `TerraDataset` / `from_api_microdata()` | `analyze_network()` | `base_period`, `verbose` | DataFrame | `Node`, `Period`, network metrics, optional indices | Builds graph by period |
| Use precomputed metrics | Precomputed metrics | `NetworkMetricsDataset.from_api()` / `from_csv()` | `analyze_network()` | `base_period` | DataFrame | `Node`, `Period`, metric columns, optional indices | Skips graph construction |
| Basket over quantity | Trade microdata with qty | `TerraDataset` | `analyze_basket()` | `country`, `partner`, `product`, `direction`, `measure="qty"` | DataFrame | `period`, `qty` | Default measure is qty |
| Basket over value | Trade microdata with value | `TerraDataset` | `analyze_basket()` | `country`, `partner`, `product`, `direction`, `measure="value"` | DataFrame | `period`, `value` | Does not require qty |
| Aggregated API time series | Aggregated time series | `TimeSeriesDataset.from_api()` | `analyze_series()` | `flow`, `break_date`, `plot` | Dict | `data`, `results`, `models`, `figure`, `axes` | Primary time-series path |
| Trade microdata series | Trade microdata | `TerraDataset` | `analyze_series()` | `country`, `partner`, `product`, `direction` | Dict | `data`, `results`, `models`, `figure`, `axes` | Backward-compatible path |
| CES shock | Trade microdata with qty and value | `TerraDataset` | `simulate_shock()` | `country_from`, `country_to`, `period`, `product`, `sigma`, `eta` | `TerraDataset` | `simulation` DataFrame columns | Requires one selected period |
| Classification lookup | Reference classifications | `get_terra_classification()` | None | `name`, `lang`, `save_path` | DataFrame | API lookup fields | Not analytical data |

## 6. User-Facing Minimal Examples

### Network Metrics

```python
from terra_package import NetworkMetricsDataset, analyze_network

metrics_ds = NetworkMetricsDataset.from_csv("path/to/network_metrics.csv")
metrics = analyze_network(metrics_ds, base_period="202401")
```

### Basket Value

```python
from terra_package import TerraDataset, analyze_basket

trade_ds = TerraDataset.from_api_microdata(
    product_class="cpa",
    period="202505",
    country="IT",
    partner="ES",
    product="00",
    flow=1,
    criterion=1,
)

basket = analyze_basket(
    trade_ds,
    country="IT",
    partner="ES",
    product="00",
    measure="value",
)
```

### Aggregated Time Series

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
    countries=["IT"],
)

out = analyze_series(ts_ds, flow=1, break_date="2025-03")
```

### CES Shock

```python
from terra_package import TerraDataset, load_trade_microdata_from_api, simulate_shock

trade_df = load_trade_microdata_from_api(
    product_class="nstr",
    period="202505",
    country="IT",
    partner=None,
    product="011",
    flow=1,
    criterion=0,
    transport=[1],
)
trade_df = (
    trade_df.groupby(["source", "target", "period", "product", "flow"], as_index=False)
    .agg({"qty": "sum", "value": "sum"})
)

trade_ds = TerraDataset.from_dataframe(
    trade_df,
    trade_to_network=True,
    mode="import",
    imp_exp=["1", "2"],
    two_values=True,
)

simulated = simulate_shock(
    trade_ds,
    country_from="CA",
    country_to="IT",
    period="202505",
    product="011",
    sigma=2,
)

simulated.simulation
```

### Classifications

```python
from terra_package import get_terra_classification

products = get_terra_classification("products_cpa", lang="en")
```
