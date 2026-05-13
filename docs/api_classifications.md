# API Classifications

TERRA API payloads use reference classifications for products and transport
types. `terra-package` exposes small lookup helpers so users can inspect and
save those tables before building API payloads.

Classification tables are lookup data only. They are not trade microdata,
precomputed network metrics or aggregated time series.

## Supported Classifications

Supported classification names:

- `products_extra`
- `products_intra`
- `products_cpa`
- `transports`

Countries, partners and reporters are not documented as supported unless their
endpoints are confirmed by notebooks or API documentation. Do not treat
`/cls/productsIntra` as a partners endpoint; it is the Intra-EU product
classification.

## Retrieve One Classification

Use `get_terra_classification()` to retrieve a single lookup table as a pandas
`DataFrame`.

```python
from terra_package import get_terra_classification

products = get_terra_classification("products_cpa", lang="en")
```

Save a classification to CSV while retrieving it:

```python
transports = get_terra_classification(
    "transports",
    lang="en",
    save_path="terra_classifications/transports.csv",
)
```

## Download Several Classifications

Use `download_terra_classifications()` to save several lookup tables to a
directory.

```python
from terra_package import download_terra_classifications

download_terra_classifications(
    output_dir="terra_classifications",
    classifications=[
        "products_extra",
        "products_intra",
        "products_cpa",
        "transports",
    ],
    lang="en",
)
```

Each downloaded classification is saved as a CSV file in `output_dir`.

## Notes

- Classification utilities help build valid TERRA API payloads.
- They should not be passed to `TerraDataset`, `NetworkMetricsDataset` or
  `TimeSeriesDataset`.
- They should not be passed to `analyze_network()`, `analyze_series()`,
  `analyze_basket()` or `simulate_shock()`.
