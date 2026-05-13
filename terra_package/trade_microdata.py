from typing import Sequence, Union

import pandas as pd


API_MICRODATA_URL = "https://api.terra.istat.it/graph/downloadData"


DEFAULT_TRADE_COLS_MAP = {
    "source": ["source", "reporterISO", "reporter", "country", "declarant", "DECLARANT_ISO"],
    "target": ["target", "partnerISO", "partner", "PARTNER_ISO"],
    "period": ["period", "date", "time", "PERIOD"],
    "product": ["product", "cmdCode", "var", "productCode", "PRODUCT"],
    "qty": ["qty", "quantity", "netWgt", "weight", "QUANTITY_IN_KG"],
    "flow": ["flow", "flowDesc", "direction", "flowCode", "FLOW"],
    "value": ["value", "primaryValue", "tradeValue", "cifvalue", "fobvalue", "VALUE_IN_EUROS"],
}


def load_trade_microdata_from_api(
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
    records_path: Union[str, Sequence[str]] = None,
    cols_map: dict = None,
    params: dict = None,
    headers: dict = None,
    timeout: int = 30,
    request_session=None,
) -> pd.DataFrame:
    """
    Download TERRA trade microdata and normalize them to package columns.

    By default this function calls the TERRA ``graph/downloadData`` endpoint,
    which returns raw trade-flow observations. It does not load precomputed
    network metrics and does not load aggregated time-series data from
    ``examples/time_series_analysis_api.ipynb``.

    Parameters
    ----------
    product_class, period, country, flow, criterion :
        Required ``graph/downloadData`` payload fields.
    partner, product, transport : optional
        Optional ``graph/downloadData`` payload fields. ``None`` means the
        dimension is not filtered; ``transport=[]`` is preserved and means all
        transport types for this endpoint.
    endpoint : str, optional
        API endpoint. Defaults to
        ``https://api.terra.istat.it/graph/downloadData``.
    payload : dict, optional
        Advanced escape hatch for callers that already built the payload. If
        supplied, it is used instead of constructing a payload from the
        explicit parameters above.
    method : {"post", "get"}, default "post"
        HTTP method used for the request.
    records_path : str or sequence of str, optional
        Key path to the list of trade records in the JSON response. If omitted,
        common top-level containers are tried.
    cols_map : dict, optional
        Mapping from internal names to source response names. For example
        ``{"source": "reporterISO", "target": "partnerISO"}``.
    params, headers, timeout : optional
        Request options forwarded to the HTTP client.
    request_session : object, optional
        Object exposing ``get`` and/or ``post`` methods, useful for tests.

    Returns
    -------
    pandas.DataFrame
        Normalized trade microdata with ``source``, ``target``, ``period``,
        ``product``, ``qty``, ``flow`` and, when available, ``value``.
    """
    endpoint = endpoint or API_MICRODATA_URL
    request_payload = (
        payload.copy()
        if payload is not None
        else build_download_data_payload(
            product_class=product_class,
            period=period,
            country=country,
            flow=flow,
            criterion=criterion,
            partner=partner,
            product=product,
            transport=transport,
        )
    )

    session = request_session if request_session is not None else _requests()
    method = method.lower()

    if method == "post":
        response = session.post(
            endpoint,
            json=request_payload,
            params=params,
            headers=headers,
            timeout=timeout,
        )
    elif method == "get":
        response = session.get(
            endpoint,
            params=params if params is not None else request_payload,
            headers=headers,
            timeout=timeout,
        )
    else:
        raise ValueError("method must be 'post' or 'get'.")

    if not response.ok:
        raise RuntimeError(
            f"Failed to fetch trade microdata from API: {response.status_code}"
        )

    try:
        response_json = response.json()
    except ValueError as exc:
        raise ValueError(
            "The graph/downloadData response could not be parsed as JSON "
            "or has an unexpected format."
        ) from exc

    return trade_microdata_response_to_dataframe(
        response_json,
        records_path=records_path,
        cols_map=cols_map,
        request_context=request_payload,
    )


def build_download_data_payload(
    product_class: str,
    period: str,
    country: str,
    flow,
    criterion,
    partner: str = None,
    product: str = None,
    transport=None,
) -> dict:
    """
    Build a ``graph/downloadData`` payload for TERRA trade microdata.
    """
    required = {
        "product_class": product_class,
        "period": period,
        "country": country,
        "flow": flow,
        "criterion": criterion,
    }
    missing = [
        name for name, value in required.items()
        if value is None or (isinstance(value, str) and value.strip() == "")
    ]
    if missing:
        raise ValueError(
            "Missing required graph/downloadData parameter(s): "
            f"{missing}."
        )

    payload = {
        "product_class": str(product_class).strip(),
        "period": str(period).strip(),
        "country": str(country).strip(),
        "flow": flow,
        "criterion": criterion,
    }

    partner = _normalize_optional_payload_value(partner)
    product = _normalize_optional_payload_value(product)
    transport = _normalize_optional_payload_value(transport)

    if partner is not None:
        payload["partner"] = str(partner)
    if product is not None:
        payload["product"] = str(product)
    if transport is not None:
        payload["transport"] = transport

    return payload


def _normalize_optional_payload_value(value):
    if isinstance(value, str) and value.strip() == "":
        return None
    return value


def trade_microdata_response_to_dataframe(
    response_json,
    records_path: Union[str, Sequence[str]] = None,
    cols_map: dict = None,
    request_context: dict = None,
) -> pd.DataFrame:
    """
    Convert an API JSON response into normalized trade microdata.

    The returned DataFrame contains internal ``TerraDataset`` column names and
    represents raw trade flows, not network metrics or aggregated time series.
    """
    records = _extract_records(response_json, records_path=records_path)
    data = pd.DataFrame(records)
    return normalize_trade_microdata(
        data,
        cols_map=cols_map,
        request_context=request_context,
    )


def normalize_trade_microdata(
    data: pd.DataFrame,
    cols_map: dict = None,
    require_value: bool = False,
    request_context: dict = None,
) -> pd.DataFrame:
    """
    Normalize a trade-microdata DataFrame to the package schema.

    Periods in ``YYYY-MM`` or datetime-like monthly form are converted to
    ``YYYYMM``. Quantity and value columns are converted to numeric values.
    """
    if data is None or data.empty:
        raise ValueError("The API trade microdata response is empty.")

    out = data.copy()
    out = out.rename(columns=_build_trade_rename_map(out.columns, cols_map))
    out = _enrich_from_request_context(out, request_context=request_context)

    required = {"source", "target", "period", "product", "qty", "flow"}
    if require_value or "value" in out.columns:
        required.add("value")

    missing = required - set(out.columns)
    if missing:
        raise ValueError(
            "Missing required API trade microdata columns after mapping: "
            f"{sorted(missing)}. Provide cols_map if the API fields use "
            "different names."
        )

    for col in ["source", "target", "product", "flow"]:
        out[col] = out[col].astype(str).str.strip()
        if out[col].eq("").any():
            raise ValueError(f"Column '{col}' contains empty values.")

    out["period"] = _normalize_period(out["period"])
    out["qty"] = _normalize_numeric(out, "qty")
    if "value" in out.columns:
        out["value"] = _normalize_numeric(out, "value")

    return out


def _enrich_from_request_context(data: pd.DataFrame, request_context: dict = None) -> pd.DataFrame:
    if not request_context:
        return data

    out = data.copy()
    context_map = {
        "source": "country",
        "period": "period",
        "flow": "flow",
        "product": "product",
        "target": "partner",
    }
    for internal_col, payload_key in context_map.items():
        if internal_col not in out.columns and payload_key in request_context:
            value = request_context[payload_key]
            if value is not None:
                out[internal_col] = value

    return out


def _extract_records(response_json, records_path=None):
    if records_path is not None:
        current = response_json
        path = records_path.split(".") if isinstance(records_path, str) else records_path
        for key in path:
            try:
                current = current[key]
            except (KeyError, TypeError) as exc:
                raise ValueError(
                    f"API response does not contain records_path {records_path!r}."
                ) from exc
        records = current
    elif isinstance(response_json, list):
        records = response_json
    elif isinstance(response_json, dict):
        records = None
        for key in ("data", "records", "items", "microdata", "trade", "results"):
            value = response_json.get(key)
            if isinstance(value, list):
                records = value
                break
        if records is None:
            records = response_json
    else:
        records = None

    if isinstance(records, dict):
        records = [records]
    if not records:
        raise ValueError("The API trade microdata response is empty.")
    if not isinstance(records, list):
        raise ValueError("The API trade microdata response must contain records.")

    return records


def _build_trade_rename_map(columns, cols_map=None):
    source_map = DEFAULT_TRADE_COLS_MAP.copy()
    if cols_map:
        source_map.update(cols_map)

    existing = set(columns)
    rename_map = {}
    for internal_name, source_name in source_map.items():
        if internal_name in existing:
            continue
        candidates = source_name if isinstance(source_name, list) else [source_name]
        for candidate in candidates:
            if candidate in existing:
                rename_map[candidate] = internal_name
                break

    return rename_map


def _normalize_period(period_series: pd.Series) -> pd.Series:
    s = period_series.astype(str).str.strip()
    s = s.str.replace(r"^(\d{4})-(\d{2})$", r"\1\2", regex=True)

    monthly_pattern = r"\d{4}(0[1-9]|1[0-2])"
    date_like = ~s.str.fullmatch(monthly_pattern)
    if date_like.any():
        parsed = pd.to_datetime(s[date_like], errors="coerce")
        if parsed.isna().any():
            invalid = s[date_like][parsed.isna()].unique()[:5]
            raise ValueError(
                "Column 'period' must use YYYYMM, YYYY-MM, or a parseable "
                f"monthly date. Invalid examples: {invalid}."
            )
        s.loc[date_like] = parsed.dt.strftime("%Y%m").values

    if not s.str.fullmatch(monthly_pattern).all():
        invalid = s[~s.str.fullmatch(monthly_pattern)].unique()[:5]
        raise ValueError(
            "Column 'period' must use valid YYYYMM-style monthly values. "
            f"Invalid examples: {invalid}."
        )

    return s


def _normalize_numeric(data: pd.DataFrame, col: str) -> pd.Series:
    cleaned = data[col]
    if pd.api.types.is_string_dtype(cleaned):
        cleaned = cleaned.str.replace(",", "", regex=False)
    converted = pd.to_numeric(cleaned, errors="coerce")
    if converted.isna().any():
        invalid = data.loc[converted.isna(), col].unique()[:5]
        raise ValueError(
            f"Column '{col}' contains non-numeric API trade microdata values. "
            f"Examples: {invalid}."
        )
    return converted


def _requests():
    try:
        import requests
    except ImportError as exc:
        raise ImportError(
            "load_trade_microdata_from_api requires the 'requests' package."
        ) from exc
    return requests
