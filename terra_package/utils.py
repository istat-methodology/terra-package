import pandas as pd

class TerraDataset:
    """
    A class to represent and validate TERRA trade datasets, with optional
    conversion to a network (graph-like) format.

    Attributes:
        trade_to_network (bool): Whether to convert the dataset into a 
            source-target network format.
        mode (str): Mode of conversion when `trade_to_network=True`. 
            Options are "import", "export", or "both".
        imp_exp (list[str]): Labels used to identify import and export flows. 
            Default is ["I", "E"].
        data (pd.DataFrame): The validated (and possibly transformed) dataset.
        two_values (bool): Whether the dataset includes a second numerical 
            column (value2).
        cols_map (dict): Optional mapping to rename columns from the raw 
            file.
        sep (str): Column separator used when reading the CSV file.
        encoding (str): File encoding used to read the dataset.
    """
    def __init__(self, path: str, trade_to_network: bool = False, mode:str = "both", imp_exp: list = None, two_values: bool = False, cols_map: dict = None, sep: str = ",", encoding: str = "utf-8"):
        """
        Initialize a TerraDataset instance.

        Args:
            path (str): Path to the CSV file to load.
            trade_to_network (bool, optional): If True, converts the dataset
                into a network format. Default is False.
            mode (str, optional): Conversion mode when `trade_to_network=True`.
                Must be one of {"import", "export", "both"}. Default is "both".
            imp_exp (list[str], optional): A two-element list indicating the 
                labels for imports and exports in the dataset. 
                Default is ["I", "E"].
            two_values (bool, optional): If True, requires and validates 
                an additional column "value2".
            cols_map (dict, optional): A dictionary for renaming columns 
                in the dataset (raw → expected names).
            sep (str, optional): Field separator used when reading the CSV file.
            encoding (str, optional): File encoding for reading the CSV file.

        Raises:
            ValueError: If the dataset does not meet validation requirements.
        """
        self.trade_to_network = trade_to_network
        self.mode = mode
        self.imp_exp = imp_exp if imp_exp is not None else ["I", "E"]
        self.two_values = two_values
        self.cols_map = cols_map
        self.sep = sep
        self.encoding = encoding
        self.required_keys = self._required_cols[0] + (self._required_cols[1] if trade_to_network else []) + (self._required_cols[2] if two_values else [])
        self.data = self._check(path)

    def _check(self, path):
        """
        Load and validate the dataset from a CSV file.

        Performs base validation checks and, if required, applies the
        trade-to-network conversion. If cols_map is provided, the 
        dataset columns are renamed before validation. This method also 
        handles validation of the optional second value column ("value2")
        when two_values=True.

        Args:
            path (str): Path to the CSV file.

        Returns:
            pd.DataFrame: The validated and possibly transformed dataset.

        Raises:
            ValueError: If the dataset does not pass validation checks.
        """
        df = pd.read_csv(path, sep=self.sep, encoding=self.encoding)
        
        if self.cols_map:
            df = self._rename_columns(df)
        
        self._base_checks(df)        
        
        if self.trade_to_network:
            df = self._trade_to_network(df)
        
        return df

    def _rename_columns(self, df: pd.DataFrame):
        """
        Rename the dataset columns according to the mapping provided by the user.
        """
        if not set(self.required_keys).issubset(self.cols_map.keys()):
            missing = set(self.required_keys) - self.cols_map.keys()
            raise ValueError(f"The provided cols_map must contain keys: {missing}")
    
        missing_cols = set(self.cols_map.values()) - set(df.columns)
        if missing_cols:
            raise ValueError(f"The dataset has no column(s) called: {missing_cols}")
        
        reverse_map = {v: k for k, v in self.cols_map.items()}
        df.rename(columns=reverse_map, inplace=True)
        return df
    
    def _base_checks(self, df: pd.DataFrame):
        """
        This method validates:
            - presence of required columns
            - absence of duplicate edges
            - numeric validity of "value" and, if two_values=True, 
            also "value2"

        Args:
            df (pd.DataFrame): The dataset to validate.

        Raises:
            ValueError: If required columns are missing, if duplicate 
            edges are found, or if numeric conversion of 'value' 
            (or 'value2' when applicable) fails.
        """
        if not set(self.required_keys).issubset(df.columns):
            raise ValueError(f"The dataframe must contain columns: {cols}")
        
        cols = [c for c in self.required_keys if c not in ['value', 'value2']]
        if (df.shape[0] != df[cols].drop_duplicates().shape[0]):
            dups = df.groupby(cols,as_index=False)["value"].count()
            dups = dups[dups["value"]>1][:3]
            raise ValueError(f"The dataframe has duplicate edges: first {dups.shape[0]} {dups.values.tolist()}...")

        if pd.api.types.is_string_dtype(df["value"]):
            df["value"] = df["value"].str.replace(',','')
            df["value"] = df["value"].str.replace('.','').astype(int)
        converted = pd.to_numeric(df["value"], errors="coerce")
        
        if converted.isna().any():
            invalid_values = df.loc[converted.isna(), "value"].unique()[:5]
            raise ValueError(f"Column 'value' contains non-numeric values. Examples: {invalid_values}...")
        
        if self.two_values:
            if pd.api.types.is_string_dtype(df["value2"]):
                df["value2"] = df["value2"].str.replace(',','')
                df["value2"] = df["value2"].str.replace('.','').astype(int)
            converted2 = pd.to_numeric(df["value2"], errors="coerce")

            if converted2.isna().any():
                invalid_values2 = df.loc[converted2.isna(), "value"].unique()[:5]
                raise ValueError(f"Column 'value' contains non-numeric values. Examples: {invalid_values2}...")
    
    def _trade_to_network(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a trade dataset into a network format.

        Depending on the mode, this method transforms the dataset so that
        trade flows are represented as edges between source and target nodes.
        This method uses self.mode and self.imp_exp to determine how trade flows
        are converted to source–target edges. For import flows, source and target
        are swapped. When mode="both", import and export edges are combined and 
        aggregated. If two_values=True, both "value" and "value2" are aggregated.

        Args:
            df (pd.DataFrame): The input trade dataset.

        Returns:
            pd.DataFrame: The transformed dataset in network format.

        Raises:
            ValueError: If the mode is invalid or if the resulting dataset
                is empty.
        """
        if self.mode == 'import':
            df = df[df['flow'] == self.imp_exp[0]][self.required_keys]
            df.loc[:, ['source', 'target']] = df[['target', 'source']].values
        elif self.mode == 'export':
            df = df[df['flow'] == self.imp_exp[1]][self.required_keys]
        elif self.mode == 'both':
            df_imp = df[df['flow'] == self.imp_exp[0]][self.required_keys]
            df.loc[:, ['source', 'target']] = df[['target', 'source']].values
            df_exp = df[df['flow'] == self.imp_exp[1]][self.required_keys]
            df = pd.concat([df_imp, df_exp], ignore_index=True)
            cols = [c for c in self.required_keys if c not in ['value', 'flow', 'value2']]
            df = df.groupby(cols, as_index=False)['value'].mean() if not self.two_values else df.groupby(cols, as_index=False).agg({'value':'mean','value2':'mean'})
        else:
            raise ValueError("mode must be 'import', 'export' or 'both'.")
        
        if df.empty:
            raise ValueError("The dataframe is empty after trade to network conversion. Check 'mode' and 'imp_exp' parameters.")
        return df
    
    # Required column groups: base columns, flow column, optional second value column
    _required_cols = [['source', 'target', 'period', 'product', 'value'],['flow'],['value2']]