from pathlib import Path
from typing import Dict, Iterable

import pandas as pd


API_CLASSIFICATIONS_BASE_URL = "https://api.terra.istat.it/cls"

TERRA_CLASSIFICATION_ENDPOINTS = {
    "products_extra": "productsExtra",
    "products_intra": "productsIntra",
    "transports": "transports",
    "products_cpa": "productsCPA",
}


def list_terra_classifications() -> list[str]:
    """
    Return the supported TERRA API reference-classification names.

    These names identify reference classifications used to build valid TERRA
    API payloads. They are not trade microdata and not precomputed network
    metrics.
    """
    return sorted(TERRA_CLASSIFICATION_ENDPOINTS)


def get_terra_classification(
    name: str,
    lang: str = "en",
    save_path: str = None,
    sep: str = ",",
    encoding: str = "utf-8",
    request_session=None,
    timeout: int = 30,
) -> pd.DataFrame:
    """
    Retrieve a TERRA API reference classification as a DataFrame.

    This utility downloads reference classifications used to build valid TERRA
    API payloads. It does not download trade microdata and does not download
    precomputed network metrics.

    Supported classification names are:
    ``products_extra``, ``products_intra``, ``transports`` and
    ``products_cpa``. The endpoints are based on
    ``examples/graph_analysis_api.ipynb`` and
    ``examples/time_series_analysis_api.ipynb``.

    Parameters
    ----------
    name : str
        Classification name, for example ``"products_cpa"``.
    lang : str, default "en"
        Language query parameter passed to the TERRA API.
    save_path : str, optional
        If provided, save the returned classification to CSV.
    sep : str, default ","
        CSV separator used when ``save_path`` is provided.
    encoding : str, default "utf-8"
        CSV encoding used when ``save_path`` is provided.
    request_session : object, optional
        Object exposing a ``get`` method. Useful for tests.
    timeout : int, default 30
        HTTP request timeout in seconds.

    Returns
    -------
    pandas.DataFrame
        Reference classification table with the fields returned by the API.

    Examples
    --------
    >>> products = get_terra_classification("products_cpa", lang="en")
    >>> transports = get_terra_classification("transports", save_path="transports.csv")
    """
    endpoint = _classification_url(name)
    session = request_session if request_session is not None else _requests()

    try:
        response = session.get(endpoint, params={"lang": lang}, timeout=timeout)
    except Exception as exc:
        raise RuntimeError(
            f"Failed to fetch TERRA classification '{name}' from {endpoint}: {exc}"
        ) from exc

    if not getattr(response, "ok", False):
        status_code = getattr(response, "status_code", "unknown")
        raise RuntimeError(
            f"Failed to fetch TERRA classification '{name}': HTTP {status_code}."
        )

    try:
        payload = response.json()
    except ValueError as exc:
        raise ValueError(
            f"TERRA classification '{name}' returned non-JSON or malformed JSON."
        ) from exc

    data = _classification_payload_to_dataframe(payload, name)

    if save_path is not None:
        path = Path(save_path)
        if path.parent and not path.parent.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
        data.to_csv(path, index=False, sep=sep, encoding=encoding)

    return data


def download_terra_classifications(
    output_dir: str,
    classifications: Iterable[str] = None,
    lang: str = "en",
    sep: str = ",",
    encoding: str = "utf-8",
    request_session=None,
    timeout: int = 30,
) -> Dict[str, pd.DataFrame]:
    """
    Download multiple TERRA reference classifications and save them to CSV.

    Parameters
    ----------
    output_dir : str
        Directory where one CSV per classification is saved.
    classifications : iterable of str, optional
        Classification names to download. If omitted, all supported
        classifications are downloaded.
    lang, sep, encoding, request_session, timeout :
        Passed to ``get_terra_classification``.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Mapping from classification name to the downloaded DataFrame.
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    names = list(classifications) if classifications is not None else list_terra_classifications()
    downloaded = {}
    for name in names:
        downloaded[name] = get_terra_classification(
            name,
            lang=lang,
            save_path=output_path / f"{name}.csv",
            sep=sep,
            encoding=encoding,
            request_session=request_session,
            timeout=timeout,
        )
    return downloaded


def _classification_url(name: str) -> str:
    if name not in TERRA_CLASSIFICATION_ENDPOINTS:
        valid = ", ".join(list_terra_classifications())
        raise ValueError(
            f"Unknown TERRA classification '{name}'. Valid values are: {valid}."
        )
    return f"{API_CLASSIFICATIONS_BASE_URL}/{TERRA_CLASSIFICATION_ENDPOINTS[name]}"


def _classification_payload_to_dataframe(payload, name: str) -> pd.DataFrame:
    records = _extract_classification_records(payload)
    if not records:
        raise ValueError(f"TERRA classification '{name}' response is empty.")

    try:
        data = pd.json_normalize(records)
    except Exception as exc:
        raise ValueError(
            f"TERRA classification '{name}' response could not be converted to a table."
        ) from exc

    if data.empty:
        raise ValueError(f"TERRA classification '{name}' response is empty.")

    return data


def _extract_classification_records(payload):
    if payload is None:
        return []
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        if not payload:
            return []
        for key in ("data", "records", "items", "results", "classifications"):
            value = payload.get(key)
            if isinstance(value, list):
                return value
        if all(not isinstance(value, (dict, list)) for value in payload.values()):
            return [payload]
    raise ValueError("TERRA classification response must contain record-like JSON.")


def _requests():
    try:
        import requests
    except ImportError as exc:
        raise ImportError(
            "get_terra_classification requires the 'requests' package."
        ) from exc
    return requests
