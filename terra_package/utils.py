import pandas as pd

from .trade_microdata import load_trade_microdata_from_api

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
            column (value).
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
                an additional column "value".
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

    @classmethod
    def from_dataframe(
        cls,
        data: pd.DataFrame,
        trade_to_network: bool = False,
        mode: str = "both",
        imp_exp: list = None,
        two_values: bool = False,
        cols_map: dict = None,
    ):
        """
        Build a TerraDataset from an in-memory trade-microdata DataFrame.

        This method follows the same validation and optional trade-to-network
        conversion used by the CSV constructor. The input must contain raw
        trade-flow observations, not precomputed network metrics.
        """
        obj = cls.__new__(cls)
        obj.trade_to_network = trade_to_network
        obj.mode = mode
        obj.imp_exp = imp_exp if imp_exp is not None else ["I", "E"]
        obj.two_values = two_values
        obj.cols_map = cols_map
        obj.sep = ","
        obj.encoding = "utf-8"
        obj.required_keys = (
            obj._required_cols[0]
            + (obj._required_cols[1] if trade_to_network else [])
            + (obj._required_cols[2] if two_values else [])
        )
        obj.data = obj._check_dataframe(data)
        return obj

    @classmethod
    def from_api_microdata(
        cls,
        product_class: str = None,
        period: str = None,
        country: str = None,
        flow=None,
        criterion=None,
        partner: str = None,
        product: str = None,
        transport=None,
        endpoint: str = None,
        payload: dict = None,
        method: str = "post",
        records_path=None,
        api_cols_map: dict = None,
        params: dict = None,
        headers: dict = None,
        timeout: int = 30,
        request_session=None,
        trade_to_network: bool = False,
        mode: str = "both",
        imp_exp: list = None,
        two_values: bool = False,
        cols_map: dict = None,
    ):
        """
        Download TERRA trade microdata and return a standard TerraDataset.

        This method calls the TERRA ``graph/downloadData`` endpoint by
        default. The response is treated as raw trade-flow observations, not as
        precomputed network metrics and not as aggregated time-series data.
        Records are normalized to the package columns ``source``, ``target``,
        ``period``, ``product``, ``flow`` and at least one of ``qty`` or
        ``value``
        before normal TerraDataset validation runs. The returned object can be
        used by ``analyze_network()``, ``analyze_basket()`` and
        ``simulate_shock()``.

        Parameters
        ----------
        product_class, period, country, flow, criterion :
            Required ``graph/downloadData`` payload fields.
        partner, product, transport : optional
            Optional ``graph/downloadData`` payload fields. ``None`` means the
            dimension is not filtered; ``transport=[]`` is preserved and means
            all transport types for this endpoint.
        endpoint, payload, method, records_path, params, headers, timeout :
            Advanced API request options passed to
            ``load_trade_microdata_from_api``.
        api_cols_map : dict, optional
            Mapping from internal trade columns to API response fields, for
            example ``{"source": "reporterISO"}``.
        request_session : object, optional
            Test-friendly object exposing ``get`` and/or ``post`` methods.
        trade_to_network, mode, imp_exp, two_values, cols_map :
            Standard TerraDataset options applied after API normalization.

        Returns
        -------
        TerraDataset
            Validated trade-microdata dataset.
        """
        data = load_trade_microdata_from_api(
            product_class=product_class,
            period=period,
            country=country,
            flow=flow,
            criterion=criterion,
            partner=partner,
            product=product,
            transport=transport,
            endpoint=endpoint,
            payload=payload,
            method=method,
            records_path=records_path,
            cols_map=api_cols_map,
            params=params,
            headers=headers,
            timeout=timeout,
            request_session=request_session,
        )
        return cls.from_dataframe(
            data,
            trade_to_network=trade_to_network,
            mode=mode,
            imp_exp=imp_exp,
            two_values=two_values,
            cols_map=cols_map,
        )

    def _check(self, path):
        """
        Load and validate the dataset from a CSV file.

        Performs base validation checks and, if required, applies the
        trade-to-network conversion. If cols_map is provided, the 
        dataset columns are renamed before validation. This method also 
        handles validation of the optional second value column ("value")
        when two_values=True.

        Args:
            path (str): Path to the CSV file.

        Returns:
            pd.DataFrame: The validated and possibly transformed dataset.

        Raises:
            ValueError: If the dataset does not pass validation checks.
        """
        df = pd.read_csv(path, sep=self.sep, encoding=self.encoding)
        return self._check_dataframe(df)

    def _check_dataframe(self, df: pd.DataFrame):
        """
        Validate an in-memory trade dataset and optionally convert it to a
        source-target network format.
        """
        df = df.copy()
        if self.cols_map:
            df = self._rename_columns(df)
        self._base_checks(df)        
        
        if self.trade_to_network:
            df = self._trade_to_network(df)
        
        return df

    def to_csv(self, path: str, sep: str = ",", encoding: str = "utf-8", index: bool = False, **kwargs):
        """
        Save the validated dataset to CSV.

        This is useful for download-only workflows: API-downloaded trade
        microdata can be saved and later reloaded with the standard
        ``TerraDataset`` CSV constructor.
        """
        return self.data.to_csv(path, sep=sep, encoding=encoding, index=index, **kwargs)

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
        
        cols_map_diff = {
            k for k, v in self.cols_map.items() if k != v
        }
        overlapping = cols_map_diff & set(df.columns)
        if overlapping:
            raise ValueError(
                f"Column renaming failed because the dataset already contains columns with the same name as the target names: {overlapping}"
            )
        
        reverse_map = {v: k for k, v in self.cols_map.items()}
        df.rename(columns=reverse_map, inplace=True)
        return df
    
    def _base_checks(self, df: pd.DataFrame):
        """
        This method validates:
            - presence of required columns
            - absence of duplicate edges
            - numeric validity of available "qty" and/or "value" columns

        Args:
            df (pd.DataFrame): The dataset to validate.

        Raises:
            ValueError: If required columns are missing, if duplicate 
            edges are found, or if numeric conversion of available measure
            columns fails.
        """
        if not set(self.required_keys).issubset(df.columns):
            missing = set(self.required_keys) - set(df.columns)
            raise ValueError(
                f"The dataframe must contain columns: {self.required_keys}. "
                f"Missing: {sorted(missing)}"
            )
        if self.two_values:
            missing_measures = {"qty", "value"} - set(df.columns)
            if missing_measures:
                raise ValueError(
                    "The dataframe must contain both 'qty' and 'value' when "
                    f"two_values=True. Missing: {sorted(missing_measures)}"
                )
        elif not {"qty", "value"}.intersection(df.columns):
            raise ValueError("The dataframe must contain at least one of 'qty' or 'value'.")

        df["period"] = self._normalize_period_labels(df["period"])
        cols = [c for c in self.required_keys if c not in ['qty', 'value']]
        if (df.shape[0] != df[cols].drop_duplicates().shape[0]):
            dups = df.groupby(cols, as_index=False).size()
            dups = dups[dups["size"] > 1][:3]
            raise ValueError(f"The dataframe has duplicate edges: first {dups.shape[0]} {dups.values.tolist()}...")

        for measure in ["qty", "value"]:
            if measure not in df.columns:
                continue
            if pd.api.types.is_string_dtype(df[measure]):
                df[measure] = df[measure].str.replace(',','')
                df[measure] = df[measure].str.replace('.','').astype(int)
            converted = pd.to_numeric(df[measure], errors="coerce")
            if converted.isna().any():
                invalid_values = df.loc[converted.isna(), measure].unique()[:5]
                raise ValueError(f"Column '{measure}' contains non-numeric values. Examples: {invalid_values}...")

    @staticmethod
    def _normalize_period_labels(period: pd.Series) -> pd.Series:
        """
        Keep periods as stable string labels after CSV loading.

        Pandas may infer monthly labels such as 202501 as integers. Downstream
        functions compare periods as labels, so integer-like values are
        converted to strings while already textual formats such as YYYY-MM are
        preserved.
        """
        def normalize_value(value):
            if pd.isna(value):
                return value
            if isinstance(value, float) and value.is_integer():
                return str(int(value))
            return str(value).strip()

        return period.map(normalize_value)

    def _trade_to_network(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Convert a trade dataset into a network format.

        Depending on the mode, this method transforms the dataset so that
        trade flows are represented as edges between source and target nodes.
        This method uses self.mode and self.imp_exp to determine how trade flows
        are converted to source–target edges. For import flows, source and target
        are swapped. When mode="both", import and export edges are combined and 
        aggregated. If two_values=True, both "qty" and "value" are aggregated.

        Args:
            df (pd.DataFrame): The input trade dataset.

        Returns:
            pd.DataFrame: The transformed dataset in network format.

        Raises:
            ValueError: If the mode is invalid or if the resulting dataset
                is empty.
        """
        measure_cols = [col for col in ["qty", "value"] if col in df.columns]
        selected_cols = list(dict.fromkeys(
            col for col in self.required_keys + measure_cols if col in df.columns
        ))
        if self.mode == 'import':
            df = df[df['flow'] == self.imp_exp[0]][selected_cols]
            df.loc[:, ['source', 'target']] = df[['target', 'source']].values
        elif self.mode == 'export':
            df = df[df['flow'] == self.imp_exp[1]][selected_cols]
        elif self.mode == 'both':
            df_imp = df[df['flow'] == self.imp_exp[0]][selected_cols]
            df.loc[:, ['source', 'target']] = df[['target', 'source']].values
            df_exp = df[df['flow'] == self.imp_exp[1]][selected_cols]
            df = pd.concat([df_imp, df_exp], ignore_index=True)
            cols = [c for c in self.required_keys if c not in ['qty', 'flow', 'value']]
            df = df.groupby(cols, as_index=False).agg({col: 'mean' for col in measure_cols})
        else:
            raise ValueError("mode must be 'import', 'export' or 'both'.")
        
        if df.empty:
            raise ValueError("The dataframe is empty after trade to network conversion. Check 'mode' and 'imp_exp' parameters.")
        return df
    
    # Required column groups: base columns, flow column, optional second value column
    _required_cols = [['source', 'target', 'period', 'product'],['flow'],['value']]
