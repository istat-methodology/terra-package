import time
from datetime import datetime, timedelta

import pandas as pd


API_GRAPH_BASE_URL = "https://api.terra.istat.it/graph/graph"
API_METADATA_URL = "https://api.terra.istat.it/cls/metadata"


class NetworkMetricsDataset:
    """
    Container for precomputed TERRA network metrics.

    This class loads already-calculated graph metrics, not trade microdata.
    It is intended for use with ``analyze_network()``, where metric
    computation from trade flows is skipped and downstream fixed-base index
    calculations can be applied directly.

    Metrics can be loaded from the TERRA Graph API workflow used in
    ``examples/graph_analysis_api.ipynb`` or from a CSV previously saved by
    the user.

    Parameters
    ----------
    data : pandas.DataFrame
        DataFrame containing precomputed network metrics.
    cols_map : dict, optional
        Mapping from internal column names to source column names. For
        example ``{"Node": "country", "Period": "period"}``.

    Attributes
    ----------
    data : pandas.DataFrame
        Validated metrics using the internal column names expected by
        ``analyze_network()``.

    Examples
    --------
    >>> metrics_ds = NetworkMetricsDataset.from_csv("network_metrics.csv")
    >>> out = analyze_network(metrics_ds, base_period="202401")
    """

    DEFAULT_COLS_MAP = {
        "Node": "country",
        "Period": "period",
        "Density": "density",
        "Degree": ["degree_weighted", "degree"],
        "Out Degree": ["out_degree_weighted", "out_degree"],
        "In Degree": ["in_degree_weighted", "in_degree"],
        "Vulnerability": "vulnerability",
        "Closeness": ["closeness_weighted", "closeness"],
        "Betweenness": ["betweenness_weighted", "betweenness"],
        "Distinctiveness": "distinctiveness",
    }

    REQUIRED_COLUMNS = {
        "Node",
        "Period",
        "Out Degree",
        "Betweenness",
        "Distinctiveness",
    }

    OPTIONAL_METRIC_COLUMNS = {
        "Density",
        "Degree",
        "In Degree",
        "Vulnerability",
        "Closeness",
    }

    def __init__(self, data: pd.DataFrame, cols_map: dict = None):
        self.data = self._normalize_metrics(data, cols_map=cols_map)

    @classmethod
    def from_csv(
        cls,
        path: str,
        sep: str = ",",
        encoding: str = "utf-8",
        cols_map: dict = None,
        **read_csv_kwargs,
    ):
        """
        Load precomputed network metrics from a CSV file.

        The CSV must contain one row per node/country and period. It may use
        either the internal metric names (``Node``, ``Period``, ``Out Degree``)
        or the API-style names from ``graph_analysis_api.ipynb`` (``country``,
        ``period``, ``out_degree``). Custom names can be supplied through
        ``cols_map``.

        This loader is for precomputed network metrics only. It does not load
        trade microdata and its result is intended for ``analyze_network()``.
        """
        data = pd.read_csv(path, sep=sep, encoding=encoding, **read_csv_kwargs)
        if data.empty:
            raise ValueError("The precomputed network metrics CSV is empty.")
        return cls(data, cols_map=cols_map)

    @classmethod
    def from_api(
        cls,
        dataset: str,
        base_payload: dict,
        start_date: str = None,
        end_date: str = None,
        frequency: str = "month",
        sleep_seconds: float = 0.5,
        verbose: bool = True,
        request_session=None,
        endpoint: str = None,
    ):
        """
        Load precomputed network metrics from the TERRA Graph API.

        This method follows the workflow in
        ``examples/graph_analysis_api.ipynb``: for each period in the selected
        range it posts ``base_payload`` plus ``period`` to the graph endpoint
        and reads the already-calculated metrics from the ``metrics`` response
        field.

        Parameters
        ----------
        dataset : str
            Graph dataset suffix, for example ``"Intra"`` or ``"Extra"``.
        base_payload : dict
            Graph API payload excluding ``period``. Typical keys are
            ``percentage``, ``transport``, ``product``, ``flow``, ``weight``,
            ``position``, ``edges`` and ``collapse``.
        start_date, end_date : str, optional
            Date range in ``YYYY-MM`` format. If omitted, the available graph
            time range is read from the TERRA metadata endpoint.
        frequency : {"month", "quarter"}, default "month"
            Period frequency. Monthly requests use the ``Month`` endpoint;
            quarterly requests use the ``Trim`` endpoint.
        sleep_seconds : float, default 0.5
            Pause between API requests.
        verbose : bool, default True
            Print endpoint and period retrieval messages.
        request_session : object, optional
            Object exposing ``get`` and ``post`` methods. Used mainly for
            tests; if omitted, ``requests`` is imported and used.
        endpoint : str, optional
            Custom graph endpoint. By default it is derived from ``dataset``
            and ``frequency``.

        Returns
        -------
        NetworkMetricsDataset
            Validated precomputed metrics ready for ``analyze_network()``.
        """
        if frequency not in {"month", "quarter"}:
            raise ValueError("frequency must be 'month' or 'quarter'.")

        session = request_session if request_session is not None else _requests()

        if start_date is None or end_date is None:
            start_date, end_date = cls.fetch_api_time_range(
                verbose=verbose,
                request_session=session,
            )

        periods = generate_time_interval(start_date, end_date, frequency)
        graph_endpoint = endpoint or (
            f"{API_GRAPH_BASE_URL}{dataset}"
            f"{'Month' if frequency == 'month' else 'Trim'}"
        )

        if verbose:
            print(f"Endpoint: {graph_endpoint}")

        metrics_list = []
        for period in periods:
            payload = base_payload.copy()
            payload["period"] = period

            if verbose:
                print(f"Retrieving data for {period}")

            response = session.post(graph_endpoint, json=payload, timeout=30)
            if not response.ok:
                raise RuntimeError(
                    f"Failed to fetch graph metrics for period {period}: "
                    f"{response.status_code}"
                )

            data = response.json().get("metrics", {})
            if data:
                metrics_list.append(metrics_to_dataframe(data, period))

            if sleep_seconds:
                time.sleep(sleep_seconds)

        if not metrics_list:
            raise RuntimeError("No precomputed network metrics retrieved from TERRA API.")

        return cls(pd.concat(metrics_list, ignore_index=True))

    @staticmethod
    def fetch_api_time_range(verbose: bool = True, request_session=None) -> tuple[str, str]:
        """
        Fetch the available graph-metrics time range from TERRA metadata.

        Returns
        -------
        tuple[str, str]
            ``(start_date, end_date)`` in ``YYYY-MM`` format.
        """
        session = request_session if request_session is not None else _requests()

        if verbose:
            print(f"Endpoint: {API_METADATA_URL}")

        response = session.get(API_METADATA_URL, timeout=30)
        if not response.ok:
            raise RuntimeError(f"Failed to fetch metadata: {response.status_code}")

        metadata = response.json()
        try:
            graph_meta = metadata["graph"]
            start_year = graph_meta["timeStart"]["year"]
            start_month = graph_meta["timeStart"]["month"]
            end_year = graph_meta["timeEnd"]["year"]
            end_month = graph_meta["timeEnd"]["month"]
        except KeyError as exc:
            raise RuntimeError(
                "Unexpected metadata format: missing graph time range."
            ) from exc

        start_date = f"{start_year:04d}-{start_month:02d}"
        end_date = f"{end_year:04d}-{end_month:02d}"

        if verbose:
            print(f"Graph time range: {start_date} -> {end_date}")

        return start_date, end_date

    @classmethod
    def _normalize_metrics(cls, data: pd.DataFrame, cols_map: dict = None) -> pd.DataFrame:
        if data is None or data.empty:
            raise ValueError("The precomputed network metrics dataset is empty.")

        out = data.copy()
        rename_map = _build_rename_map(out.columns, cols_map)
        out = out.rename(columns=rename_map)

        missing_cols = cls.REQUIRED_COLUMNS - set(out.columns)
        if missing_cols:
            raise ValueError(
                "Missing required precomputed network metric columns after "
                f"mapping: {sorted(missing_cols)}. Provide cols_map if your "
                "CSV/API fields use different names."
            )

        out["Node"] = out["Node"].astype(str).str.strip()
        out["Period"] = out["Period"].astype(str).str.strip()
        out["Period"] = out["Period"].str.replace(
            r"^(\d{4})-(\d{2})$",
            r"\1\2",
            regex=True,
        )

        if out["Node"].eq("").any():
            raise ValueError("Column 'Node' contains empty identifiers.")
        if out["Period"].eq("").any():
            raise ValueError("Column 'Period' contains empty period values.")
        valid_periods = out["Period"].str.fullmatch(r"\d{6}")
        if not valid_periods.all():
            invalid = out.loc[~valid_periods, "Period"].unique()[:5]
            raise ValueError(
                "Column 'Period' must use YYYYMM-style values, or YYYY-MM "
                f"values that can be converted. Invalid examples: {invalid}."
            )

        numeric_columns = (
            (cls.REQUIRED_COLUMNS | cls.OPTIONAL_METRIC_COLUMNS)
            & set(out.columns)
            - {"Node", "Period"}
        )
        for col in numeric_columns:
            converted = pd.to_numeric(out[col], errors="coerce")
            if converted.isna().any():
                invalid_values = out.loc[converted.isna(), col].unique()[:5]
                raise ValueError(
                    f"Column '{col}' contains non-numeric metric values. "
                    f"Examples: {invalid_values}."
                )
            out[col] = converted

        return out


def generate_time_interval(start_date: str, end_date: str, frequency: str) -> list[str]:
    """
    Generate graph API periods between two ``YYYY-MM`` dates.

    Monthly frequency returns ``YYYYMM`` periods. Quarterly frequency mirrors
    the notebook helper and returns ``YYYYQQ`` periods where the quarter is
    zero-padded.
    """
    if frequency not in {"month", "quarter"}:
        raise ValueError("frequency must be 'month' or 'quarter'.")

    start = datetime.strptime(start_date, "%Y-%m")
    end = datetime.strptime(end_date, "%Y-%m")
    if start > end:
        raise ValueError("start_date must be earlier than or equal to end_date.")

    periods = []
    current = start
    while current <= end:
        if frequency == "quarter":
            quarter = (current.month - 1) // 3 + 1
            period = f"{current.year}{quarter:02d}"
            if period not in periods:
                periods.append(period)
        else:
            periods.append(current.strftime("%Y%m"))

        current += timedelta(days=32)
        current = current.replace(day=1)

    return periods


def metrics_to_dataframe(json_data: dict, period: str) -> pd.DataFrame:
    """
    Convert the TERRA Graph API ``metrics`` object into a DataFrame.
    """
    if not json_data:
        raise ValueError("The API metrics response is empty.")

    metrics = pd.DataFrame(json_data).reset_index()
    metrics = metrics.rename(columns={"index": "country"})
    metrics["period"] = period
    return metrics


def _build_rename_map(columns, cols_map: dict = None) -> dict:
    source_map = NetworkMetricsDataset.DEFAULT_COLS_MAP.copy()
    if cols_map:
        source_map.update(cols_map)

    rename_map = {}
    existing = set(columns)
    for internal_name, source_name in source_map.items():
        if internal_name in existing:
            continue
        candidates = source_name if isinstance(source_name, list) else [source_name]
        for candidate in candidates:
            if candidate in existing:
                rename_map[candidate] = internal_name
                break

    return rename_map


def _requests():
    try:
        import requests
    except ImportError as exc:
        raise ImportError(
            "NetworkMetricsDataset.from_api requires the 'requests' package."
        ) from exc
    return requests
