# Terra-package

A Python package for performing network analysis and time series aggregation on dataframes with columns: `source`, `target`, `period`, and `weight`. The package calculates network metrics for each node and returns a new dataframe with the results.

---

## **Installation**

You can install the package locally using `pip`:

git clone <your-repo-url> 
cd terra-package
pip install -e .


## Requirements
Python >= 3.8
pandas >= 1.0
networkx >= 2.0

## Usage
The `terra-package` provides three main functionalities: a function for **network** analysis, a function for **basket time series** analysis and a function for **simulation**.

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

```python
cols_map = {
    "source": "reporterISO",
    "target": "partner",
    "period": "period",
    "product": "cmdCode",
    "weight": "primaryValue"
}

terra_ds = TerraDataset(url, cols_map = cols_map)
```

As with Comext data, trading datasets often consist of individual countries' import and export data. Therefore, data must be harmonized to achieve a network structure. To do this, the trade_to_network=True parameter can be used to process it, requiring the presence of the flow column (which can also be referenced with the cols_map parameter). Along with this parameter, you can also specify the data processing method: mode=import considers only the import data, mode=export considers only the export data, and mode=both considers both data, calculating the average weight in the event of duplication between trades. Finally, the imp_exp parameter allows the user to specify how to select the import and export data, respectively.
Here are some examples:

```python
cols_map = {
    "source": "declarant",
    "target": "partner",
    "period": "period",
    "product": "product",
    "weight": "value",
    "flow": "direction"
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

Below is an example of its use:

```python
from terra_package.core import analyze_network, analyze_basket
analyze_network(terra_ds)
```

### Basket time series
With this package, it is possible to create time series starting from trade data. You must indicate the country you wish to analyze. Optionally, you can specify a second country to observe a specific link in time, the direction in case you want to see importation or exportation of that country, a specific product and choose to view the raw data or the percentage change compared to the previous month.
Below some example:

```python
# time series of the exportation raw data for country A, on all products and on all trades
analyze_basket(terra_ds, country="A")

# time series of the exportationraw data for country A and country B, on all products
analyze_basket(terra_ds, country="A", partner="B")

# time series of the exportationraw data for country B and country D, on product x
analyze_basket(terra_ds, country="B", partner="D", product="x")

# time series of the exportation percentage change for country E, on all products and on all trades
analyze_basket(terra_ds, country="E", var=True)

# time series of the importation raw data for country E, on all products and on all trades
analyze_basket(terra_ds, country="F", direction="I", var=False)
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

terra_ds = TerraDataset("../trade_sample.csv")

# Shock: remove country A as supplier to country B in period "2020M01"
simulated = simulate_shock(
    df=terra_ds,
    country_from="A",
    country_to="B",
    period="2020M01"
)

simulated.simulation
```
