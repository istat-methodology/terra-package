from typing import Sequence, Union

import pandas as pd


API_TIME_SERIES_URL = "https://api.terra.istat.it/time-series/ts"


class TimeSeriesDataset:
    """
    Container for aggregated TERRA time-series data.

    ``TimeSeriesDataset`` stores data that are already aggregated over time for
    a selected country, partner, product and flow. It is intended for
    ``analyze_series()`` only. These data are not trade microdata and should not
    be passed to ``TerraDataset``, ``analyze_network()``, ``analyze_basket()``
    or ``simulate_shock()``.

    The normalized dataframe always contains ``date`` as a pandas datetime
    column and ``period`` as a ``YYYYMM`` string label. At least one numeric
    series column such as ``series``, ``value``, ``qty`` or ``unit_value`` must
    be present. Metadata columns such as ``country``, ``partner``, ``product``,
    ``flow``, ``data_type`` and ``tipovar`` are preserved when available.
    Duplicate monthly observations are checked using the canonical series key:
    ``country``, ``partner``, ``product``, ``flow``, ``data_type`` and
    ``tipovar``.

    Examples
    --------
    >>> ts_ds = TimeSeriesDataset.from_csv("time_series.csv")
    >>> out = analyze_series(ts_ds, break_date="2025-03")
    """

    DEFAULT_COLS_MAP = {
        "date": ["date", "Date", "time", "Time"],
        "period": ["period", "Period"],
        "series": ["series", "Series"],
        "value": ["value", "Value"],
        "qty": ["qty", "quantity", "Quantity"],
        "unit_value": ["unit_value", "unitValue", "Unit Value"],
        "country": ["country", "Country"],
        "partner": ["partner", "Partner"],
        "product": ["product", "var", "Product"],
        "flow": ["flow", "Flow"],
        "data_type": ["data_type", "dataType"],
        "tipovar": ["tipovar"],
    }

    NUMERIC_COLUMNS = ["series", "value", "qty", "unit_value"]

    def __init__(
        self,
        data: pd.DataFrame,
        cols_map: dict = None,
        allow_duplicates: bool = False,
        preserve_metadata_strings: bool = False,
    ):
        self.allow_duplicates = allow_duplicates
        self.preserve_metadata_strings = preserve_metadata_strings
        self.data = self._normalize(data, cols_map=cols_map)

    @classmethod
    def from_dataframe(
        cls,
        data: pd.DataFrame,
        cols_map: dict = None,
        allow_duplicates: bool = False,
    ):
        """
        Build a ``TimeSeriesDataset`` from an in-memory dataframe.

        The dataframe must contain a date-like or period-like column and at
        least one numeric series column, for example ``series``, ``value``,
        ``qty`` or ``unit_value``. Extra metadata columns are kept.
        """
        return cls(data, cols_map=cols_map, allow_duplicates=allow_duplicates)

    @classmethod
    def from_csv(
        cls,
        path: str,
        sep: str = ",",
        encoding: str = "utf-8",
        cols_map: dict = None,
        allow_duplicates: bool = False,
        **read_csv_kwargs,
    ):
        """
        Load aggregated time-series data from a CSV file.

        ``cols_map`` maps internal names to source column names, for example
        ``{"date": "Date", "series": "Value", "country": "Country"}``.
        Files already using the normalized column names do not need a mapping.
        """
        read_csv_kwargs.setdefault("dtype", str)
        data = pd.read_csv(path, sep=sep, encoding=encoding, **read_csv_kwargs)
        return cls(
            data,
            cols_map=cols_map,
            allow_duplicates=allow_duplicates,
            preserve_metadata_strings=True,
        )

    @classmethod
    def from_api(
        cls,
        base_payload: dict = None,
        countries: Sequence[str] = None,
        country: Union[str, Sequence[str]] = None,
        endpoint: str = None,
        response_path: Union[str, Sequence[str]] = "diagMain",
        request_session=None,
        params: dict = None,
        headers: dict = None,
        timeout: int = 30,
        cols_map: dict = None,
        sleep_seconds: float = 0,
        verbose: bool = False,
        **payload_kwargs,
    ):
        """
        Download aggregated time series from the TERRA time-series API.

        This method follows ``examples/time_series_analysis_api.ipynb``. It
        posts ``base_payload`` plus one ``country`` at a time to
        ``https://api.terra.istat.it/time-series/ts``, reads the ``diagMain``
        response field and normalizes rows such as ``date``/``series`` into a
        ``TimeSeriesDataset``.

        Parameters
        ----------
        base_payload : dict, optional
            Time-series API payload excluding country. Typical keys are
            ``flow``, ``var``, ``partner``, ``dataType`` and ``tipovar``.
        countries, country : sequence or str, optional
            Countries to request. ``country`` is accepted for single-country
            calls or as an alias for ``countries``.
        endpoint : str, optional
            API endpoint. Defaults to the TERRA time-series endpoint.
        response_path : str or sequence, default "diagMain"
            JSON key path containing the time-series records.
        request_session : object, optional
            Object exposing ``post``; useful for tests.
        payload_kwargs :
            Additional payload fields merged into ``base_payload``.
        """
        request_payload = (base_payload or {}).copy()
        request_payload.update(payload_kwargs)
        country_list = _normalize_country_list(countries=countries, country=country)

        if not country_list:
            raise ValueError("TimeSeriesDataset.from_api requires at least one country.")

        session = request_session if request_session is not None else _requests()
        endpoint = endpoint or API_TIME_SERIES_URL
        frames = []

        for item in country_list:
            payload = request_payload.copy()
            payload["country"] = item

            if verbose:
                print(f"Retrieving time series for {item}")

            response = session.post(
                endpoint,
                json=payload,
                params=params,
                headers=headers,
                timeout=timeout,
            )
            if not response.ok:
                raise RuntimeError(
                    f"Failed to fetch time series for {item}: {response.status_code}"
                )

            try:
                response_json = response.json()
            except ValueError as exc:
                raise ValueError(
                    "The time-series API response could not be parsed as JSON "
                    "or has an unexpected format."
                ) from exc

            records = _extract_records(response_json, response_path=response_path)
            frame = pd.DataFrame(records)
            if frame.empty:
                raise ValueError(f"The time-series API response for {item} is empty.")

            frame["country"] = item
            _add_payload_metadata(frame, payload)
            frames.append(frame)

            if sleep_seconds:
                import time
                time.sleep(sleep_seconds)

        if not frames:
            raise RuntimeError("No time series retrieved from TERRA API.")

        return cls(pd.concat(frames, ignore_index=True), cols_map=cols_map)

    def to_csv(self, path: str, sep: str = ",", encoding: str = "utf-8", index: bool = False, **kwargs):
        """
        Save normalized aggregated time-series data to CSV.

        The exported file can be reloaded later with
        ``TimeSeriesDataset.from_csv()``.
        """
        return self.data.to_csv(path, sep=sep, encoding=encoding, index=index, **kwargs)

    def _normalize(self, data: pd.DataFrame, cols_map: dict = None) -> pd.DataFrame:
        if data is None or data.empty:
            raise ValueError("The aggregated time-series dataset is empty.")

        out = data.copy()
        out = out.rename(columns=_build_rename_map(out.columns, cols_map))

        if "date" not in out.columns and "period" not in out.columns:
            raise ValueError("TimeSeriesDataset requires a date or period column.")

        if "date" in out.columns:
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            if out["date"].isna().any():
                invalid = data.loc[out["date"].isna()].head(5).to_dict("records")
                raise ValueError(
                    "TimeSeriesDataset contains date values that cannot be parsed. "
                    f"Examples: {invalid}."
                )
            out["period"] = out["date"].dt.strftime("%Y%m")
        else:
            out["period"] = _normalize_period(out["period"])
            out["date"] = pd.to_datetime(out["period"], format="%Y%m")

        numeric_cols = [col for col in self.NUMERIC_COLUMNS if col in out.columns]
        if not numeric_cols:
            raise ValueError(
                "TimeSeriesDataset requires at least one numeric series column "
                "such as 'series', 'value', 'qty' or 'unit_value'."
            )

        for col in numeric_cols:
            cleaned = out[col]
            if pd.api.types.is_string_dtype(cleaned):
                cleaned = cleaned.str.replace(",", "", regex=False)
            converted = pd.to_numeric(cleaned, errors="coerce")
            if converted.isna().any():
                invalid_values = out.loc[converted.isna(), col].unique()[:5]
                raise ValueError(
                    f"Column '{col}' contains non-numeric time-series values. "
                    f"Examples: {invalid_values}."
                )
            out[col] = converted

        if self.preserve_metadata_strings:
            for col in ["country", "partner", "product", "flow", "data_type", "tipovar"]:
                if col in out.columns:
                    out[col] = out[col].map(
                        lambda value: value if pd.isna(value) else str(value).strip()
                    )

        if not self.allow_duplicates:
            series_key = _series_key_columns(out)
            duplicate_mask = out.duplicated(["period", *series_key], keep=False)
            if duplicate_mask.any():
                examples = out.loc[duplicate_mask, ["period", *series_key]].head(5)
                raise ValueError(
                    "TimeSeriesDataset contains duplicated time observations for "
                    f"the same series key. Examples: {examples.to_dict('records')}."
                )

        leading = ["date", "period"]
        ordered = leading + [c for c in out.columns if c not in leading]
        return out[ordered].sort_values(["period", *_series_key_columns(out)]).reset_index(drop=True)


def _normalize_country_list(countries=None, country=None) -> list[str]:
    value = countries if countries is not None else country
    if value is None:
        return []
    if isinstance(value, str):
        return [value]
    return list(value)


def _extract_records(response_json, response_path="diagMain"):
    current = response_json
    path = response_path.split(".") if isinstance(response_path, str) else response_path
    for key in path:
        try:
            current = current[key]
        except (KeyError, TypeError) as exc:
            raise ValueError(
                f"Time-series API response does not contain response_path {response_path!r}."
            ) from exc

    if isinstance(current, dict) and not any(isinstance(value, list) for value in current.values()):
        current = [current]
    if not current:
        raise ValueError("The time-series API response is empty.")
    if not isinstance(current, (list, dict)):
        raise ValueError("The time-series API response must contain records.")
    return current


def _add_payload_metadata(frame: pd.DataFrame, payload: dict) -> None:
    metadata_map = {
        "partner": "partner",
        "var": "product",
        "product": "product",
        "flow": "flow",
        "dataType": "data_type",
        "data_type": "data_type",
        "tipovar": "tipovar",
    }
    for payload_key, column in metadata_map.items():
        if column not in frame.columns and payload_key in payload:
            frame[column] = payload[payload_key]


def _build_rename_map(columns, cols_map: dict = None) -> dict:
    source_map = TimeSeriesDataset.DEFAULT_COLS_MAP.copy()
    if cols_map:
        source_map.update(cols_map)

    existing = set(columns)
    rename_map = {}
    for internal_name, source_name in source_map.items():
        if internal_name in existing:
            continue
        candidates = source_name if isinstance(source_name, list) else [source_name]
        for candidate in candidates:
            if candidate in existing and candidate not in rename_map:
                rename_map[candidate] = internal_name
                break
    return rename_map


def _normalize_period(period_series: pd.Series) -> pd.Series:
    s = period_series.astype(str).str.strip()
    s = s.str.replace(r"^(\d{4})-(\d{2})$", r"\1\2", regex=True)
    parsed = pd.to_datetime(s, format="%Y%m", errors="coerce")
    invalid = parsed.isna()
    if invalid.any():
        fallback = pd.to_datetime(s[invalid], errors="coerce")
        if fallback.isna().any():
            bad = s[invalid][fallback.isna()].unique()[:5]
            raise ValueError(
                "TimeSeriesDataset period values must use YYYYMM, YYYY-MM, "
                f"or parseable monthly dates. Invalid examples: {bad}."
            )
        s.loc[invalid] = fallback.dt.strftime("%Y%m").values
    return s


def _series_key_columns(data: pd.DataFrame) -> list[str]:
    canonical = ["country", "partner", "product", "flow", "data_type", "tipovar"]
    return [col for col in canonical if col in data.columns]


def _requests():
    try:
        import requests
    except ImportError as exc:
        raise ImportError(
            "TimeSeriesDataset.from_api requires the 'requests' package."
        ) from exc
    return requests
