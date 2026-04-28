<img src="assets/logo.png" alt="Logo">

A Python package for performing network analysis, time series aggregation and analysis and Constant Elasticity of Substitution (CES) simulation on trade dataframes.

---

## **Installation**

You can install the package locally using `pip`:

git clone https://github.com/istat-methodology/terra-package <br />
cd terra-package <br />
pip install -e .


## Requirements
Python >= 3.8 <br />
pandas >= 1.0 <br />
networkx >= 2.0 <br />
distinctiveness>=0.1.5

## Usage
The `terra-package` provides four main functionalities: 
- **network** analysis, 
- **basket time series** analysis
- **statistical time series** analysis
- **CES shock simulation**.

The repository also includes an [`examples/`](/home/mauro/projects/terra-package/examples) folder with Jupyter notebooks showing API-based analytical workflows and reusable helper code for working with TERRA data programmatically.

### Read dataset
The first step is to read a CSV file that meets minimum requirements and has certain characteristics.If the minimum requirements are not met, a series of errors are displayed to the user, with instructions on how to resolve them. Only if the error is successful can the subsequent metric calculation functions be used.
The user is given the option to upload a dataset that has the characteristics of a network, with at least the following columns:
- source
- target
- period
- product
- weight

```python
from terra_package.utils import TerraDataset

url="sample/com_trade_sample.csv"
terra_ds = TerraDataset(url)
```

The user can use the optional cols_map parameter to reference the column names in case they have different names.

As with Comext or Comtrade data, trading datasets often consist of individual countries' import and export data. Therefore, data must be harmonized to achieve a network structure. To do this, the trade_to_network=True parameter can be used to process it, requiring the presence of the flow column (which can also be referenced with the cols_map parameter). Along with this parameter, you can also specify the data processing method: mode=import considers only the import data, mode=export considers only the export data, and mode=both considers both data, calculating the average weight in the event of duplication between trades. Finally, the imp_exp parameter allows the user to specify how to select the import and export data, respectively.
Here are some examples:

```python
cols_map = {
    "source": "reporterISO",
    "target": "partnerISO",
    "period": "period",
    "product": "cmdCode",
    "qty": "qty",
    "flow": "flowDesc",
    "value": "primaryValue"
}

# Reading URL, with specified column mapping, of trading type, with 'both' mode in which the import and export values ​​in the flow column are selectable with the values ​​'Import' and 'Export'
terra_ds = TerraDataset(url, cols_map = cols_map, trade_to_network=True, mode="both", imp_exp=["Import","Export"])

# Reading URL, with no specified column mapping, of trading type, only 'import' data in which the import and export values ​​in the flow column are selectable with the default values ​('I' and 'E')
terra_ds = TerraDataset(url, trade_to_network=True, mode="import")

# Reading URL, with specified column mapping, of trading type, only export data in which the export values ​​in the flow column are selectable with the default values ​('E')
terra_ds = TerraDataset(url, cols_map = cols_map, trade_to_network=True, mode="export")
```

Finally, some technical utility functions allow you to read different CSV structures: the user is given the option to specify the column separator and data encoding.
Below are some examples:

```python
# Reading URL, with no specified column mapping, of network-ready data type (default), with semi-colon separator and 'latin-1' enconding
terra_ds = TerraDataset(url, sep=";", encoding="latin1")

# Reading URL, with no specified column mapping, of network-ready data type (default), with comma separator (default) and 'utf8' enconding
terra_ds = TerraDataset(url, encoding="utf8")

# Reading URL, with no specified column mapping, of network-ready data type (default), with tabular separator and 'utf8' enconding (default)
terra_ds = TerraDataset(url, sep="\t")
```

### Network analysis
The package provides a function for node-level network analysis. The following weighted centrality metrics are calculated:
- Degree
- Out Degree
- In Degree
- Vulnerability
- Closeness
- Betweenness
- Distinctiveness

Normalization & synthetic index: when the input dataset contains a time series dimension, fixed-base normalization is performed only if the user explicitly specifies a base period. In this case, selected network metrics are converted into fixed-base indices (e.g. Jan 2021 = 100) to ensure comparability over time. By default, the framework includes three metrics—Out Degree, Betweenness, and Distinctiveness—as a representative example of export positioning; however, these can be customized by the user. A synthetic index is then computed as their arithmetic mean, where values <100 indicate deterioration relative to the base period and values >100 indicate improvement. If no base period is provided, the function returns only the original network metrics.

Below is an example of its use:

```python
from terra_package.core import analyze_network
url="sample/com_trade_sample.csv"
terra_ds = TerraDataset(url, sep=";", encoding="latin1", cols_map=cols_map, trade_to_network=True, imp_exp=["Import","Export"], two_values=True)
analyze_network(terra_ds)

# fixed-base and synthetic index calculation
url="sample/com_trade_months.csv"
terra_ds = TerraDataset(url, sep=";", encoding="latin1", cols_map=cols_map, trade_to_network=True, imp_exp=["Import","Export"], two_values=True)
analyze_network(terra_ds, base_period='202001')
```

### Basket time series
With this package, it is possible to create time series starting from trade data. You must indicate the country you wish to analyze. Optionally, you can specify a second country to observe a specific link in time, the direction in case you want to see importation or exportation of that country, a specific product and choose to view the raw data or the percentage change compared to the previous month.
Below some example:

```python
from terra_package.core import analyze_basket
url="sample/com_trade_sample.csv"
terra_ds = TerraDataset(url, sep=";", encoding="latin1", cols_map=cols_map, trade_to_network=True, imp_exp=["Import","Export"], two_values=True)

# time series of the exportation raw data for country A, on all products and on all trades
analyze_basket(terra_ds, country="CAN")

# time series of the exportationraw data for country A and country B, on all products
analyze_basket(terra_ds, country="CAN", partner="LTU")

# time series of the exportationraw data for country B and country D, on product x
analyze_basket(terra_ds, country="CAN", partner="LTU", product="TOTAL")

# time series of the exportation percentage change for country E, on all products and on all trades
analyze_basket(terra_ds, country="CAN", var=True)

# time series of the importation raw data for country E, on all products and on all trades
analyze_basket(terra_ds, country="CAN", direction="I", var=False)
```

### Time series analysis
This function extends the basket time series functionality by providing a reproducible framework for the analysis of individual trade flows. Starting from a TerraDataset, the function:
- builds monthly series of trade value, quantity, and unit value;
- computes 12-month moving averages;
- estimates STL-based trends;
- optionally estimates a single-break model with Newey–West standard errors.

This provides an example workflow to analyze export or import dynamics over time, detect structural breaks, and visualize both raw series and smoothed trends.

Below a usage example:

```python
from terra_package.core import analyze_series

url="sample/terra_data.csv"
terra_ds = TerraDataset(url, cols_map = cols_map, trade_to_network=False, two_values=True)

out = analyze_series(
    df=terra_ds,
    country="IT",
    partner="USA",
    product="24",
    break_date="2025-03",
    plot=True
)

# data, MA12 and STL ouput
df_ts = out["data"]

# model estimation and results
model = out["models"]["value"]

res_val = out["results"]["value"]
res_qty = out["results"]["qty"]
res_price = out["results"]["unit_value"]
```

### Simulation
The package includes a simulation tool to evaluate the impact of a trade shock in which a specific supplier country is removed from the set of exporters to a given target country.

This method applies a **CES (Constant Elasticity of Substitution) demand system** to compute how import shares and traded quantities adjust after the removal of one supplier.

#### **When to use this function**
Use `simulate_shock()` when you want to:
- analyze how dependent an importing country (`country_to`) is on a specific supplier (`country_from`);
- measure substitution effects across suppliers;
- compute changes in market shares and import quantities under CES preferences.

#### **What the function does**
Given a selected `period` (and optionally a `product`):
1. Filters the trade dataset for the selected period and product.
2. Computes CES demand parameters:
   - prices  
   - α (preference weights)  
   - baseline shares  
   - baseline quantities  
3. Removes the selected supplier by setting its CES preference weight to zero.
4. Recomputes:
   - new import shares  
   - new equilibrium quantities  
   - changes in quantities (Δq)  
5. Stores the full simulation results in `terra_ds.simulation`.

If the shocked supplier is the **sole exporter** for a product, the function raises an error because no substitution is possible.

Here an example:

```python
from terra_package.core import simulate_shock

url="sample/com_trade_sample.csv"
terra_ds = TerraDataset(url)

# Shock: remove country A as supplier to country B in period "2020M01"
simulated = simulate_shock(terra_ds, country_from="ROU",country_to="ESP", period=202501)

simulated.simulation
```
