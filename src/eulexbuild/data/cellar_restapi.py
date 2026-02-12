import logging
from urllib.parse import quote

import requests
from lxml import html
from requests.adapters import HTTPAdapter
from urllib3 import Retry

from eulexbuild.utils import validate_celex, ForwardHandler

DEFAULT_TIMEOUT = 20
DEFAULT_MAX_RETRIES = 5
DEFAULT_BACKOFF_FACTOR = 1.0

_session: requests.Session | None = None


class DataRetrievalError(Exception):
    pass


class APIRequestError(DataRetrievalError):
    pass


def _create_session_with_retries(
        max_retries: int = DEFAULT_MAX_RETRIES,
        backoff_factor: float = DEFAULT_BACKOFF_FACTOR
) -> requests.Session:
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    return session


def _get_celex_url(celex_id: str) -> str:
    celex_id = validate_celex(celex_id)
    encoded_celex = quote(celex_id, safe="")
    return f"http://publications.europa.eu/resource/celex/{encoded_celex}"


def get_session() -> requests.Session:
    global _session
    if _session is None:
        _session = _create_session_with_retries()
    return _session


def close_session():
    global _session
    if _session:
        _session.close()
        _session = None


def _score_candidate(item, include_keywords: set[str] = None):
    """
    Score a document candidate for selection.

    Args:
        item: Tuple of (url, stream_name, stream_order)
        include_keywords: Set of keywords that strongly indicate the desired document

    Returns:
        Score (lower is better)
    """
    url, stream_name, stream_order = item
    score = stream_order  # Lower is better

    # Strong preference for files containing include keywords (subtract 1000 to prioritize)
    if include_keywords:
        stream_name_upper = stream_name.upper()
        for keyword in include_keywords:
            if keyword.upper() in stream_name_upper:
                score -= 1000
                break

    return score


def _parse_300_response(html_content: bytes) -> list[tuple[str, str, int]]:
    """
    Parse HTTP 300 Multiple Choices response HTML to extract document alternatives.

    Returns a list of tuples: (url, stream_name, stream_order)
    """
    try:
        tree = html.fromstring(html_content)
        items = []

        item_elements = tree.xpath('//li[@title="item"]')

        for item in item_elements:
            url_elements = item.xpath('.//a/@href')
            url = url_elements[0] if url_elements else None

            stream_name_elements = item.xpath('.//li[@title="stream_name"]/text()')
            stream_name = stream_name_elements[0].strip() if stream_name_elements else ""

            stream_order_elements = item.xpath('.//li[@title="stream_order"]/text()')
            try:
                stream_order = int(stream_order_elements[0].strip()) if stream_order_elements else 999
            except (ValueError, IndexError):
                stream_order = 999

            if url:
                items.append((url, stream_name, stream_order))

        return items
    except Exception as e:
        raise APIRequestError(f"Failed to parse 300 Multiple Choices response: {str(e)}")


def _select_document(
        items: list[tuple[str, str, int]],
        include_keywords: set[str] = None,
        exclude_keywords: set[str] = None,
        logger: logging.Logger = None
) -> str:
    """
    Select the document from multiple choices using heuristics.

    Heuristics (in priority order):
    1. Exclude files containing any exclude keywords (e.g., 'annexe', 'cover', 'erratum')
    2. Prefer files containing include keywords (e.g., 'ACT' for main documents)
    3. Prefer lowest stream_order

    Args:
        items: List of (url, stream_name, stream_order) tuples
        include_keywords: Set of keywords that strongly indicate the desired document (e.g., {'ACT'}). Defaults to empty set.
        exclude_keywords: Set of keywords to exclude (e.g., {'annexe', 'annex', 'cover', 'erratum'}). Defaults to empty set.
        logger: Optional logger for warnings

    Returns:
        URL of the selected document
    """
    if not items:
        raise APIRequestError("No documents found in 300 Multiple Choices response")

    if exclude_keywords is None:
        exclude_keywords = set()
    if include_keywords is None:
        include_keywords = set()

    candidates = []
    for url, stream_name, stream_order in items:
        stream_name_lower = stream_name.lower()
        if not any(keyword.lower() in stream_name_lower for keyword in exclude_keywords):
            candidates.append((url, stream_name, stream_order))

    if not candidates:
        if logger:
            logger.warning(
                f"No main document found after filtering (all items appear to be annexes/supplementary). "
                f"Falling back to first document: {items[0][1]}"
            )
        return items[0][0]

    best_candidate = min(candidates, key=lambda item: _score_candidate(item, include_keywords))

    if logger:
        logger.debug(f"Selected document from 300 response: {best_candidate[1]} (order: {best_candidate[2]})")

    return best_candidate[0]


def get_request(url: str, headers: dict, logger: logging.Logger = None) -> bytes:
    session = get_session()

    # Forward logging of requests/urllib3 to the specified logger
    if logger is not None:
        urlib3_logger = logging.getLogger("urllib3")
        urlib3_logger.setLevel(logging.DEBUG)
        urlib3_logger.addHandler(ForwardHandler(logger))

    try:
        response = session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)

        # Check for successful response
        if response.status_code == 404:
            raise APIRequestError(f"Document not found for {url} with headers {headers}.")
        elif response.status_code == 403:
            raise APIRequestError(f"Access forbidden for document {url} with headers {headers}.")
        elif response.status_code >= 400:
            raise APIRequestError(
                f"API request failed with status {response.status_code}: {response.reason}"
            )

        return response.content

    except requests.exceptions.Timeout:
        raise APIRequestError(f"Request timed out after {DEFAULT_TIMEOUT}s for document {url} with headers {headers}")
    except requests.exceptions.ConnectionError as e:
        raise APIRequestError(f"Connection error while fetching {url} with headers {headers}: {str(e)}")
    except requests.exceptions.RequestException as e:
        raise APIRequestError(f"Request failed for {url} with headers {headers}: {str(e)}")


def _get_request_with_300_handling(
        url: str,
        headers: dict,
        include_keywords: set[str] = None,
        exclude_keywords: set[str] = None,
        logger: logging.Logger = None
) -> bytes:
    session = get_session()

    # Forward logging of requests/urllib3 to the specified logger
    if logger is not None:
        urlib3_logger = logging.getLogger("urllib3")
        urlib3_logger.setLevel(logging.DEBUG)
        urlib3_logger.addHandler(ForwardHandler(logger))

    try:
        response = session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)

        if response.status_code == 300:
            if logger:
                logger.debug(f"Received 300 Multiple Choices response for {url}")

            items = _parse_300_response(response.content)
            if logger:
                logger.debug(f"Found {len(items)} alternatives in 300 response")

            selected_url = _select_document(
                items,
                include_keywords=include_keywords,
                exclude_keywords=exclude_keywords,
                logger=logger
            )
            if logger:
                logger.info(f"Redirecting to selected document: {selected_url}")

            response = session.get(selected_url, headers=headers, timeout=DEFAULT_TIMEOUT)

        # Check for successful response
        if response.status_code == 404:
            raise APIRequestError(f"Document not found for {url} with headers {headers}.")
        elif response.status_code == 403:
            raise APIRequestError(f"Access forbidden for document {url} with headers {headers}.")
        elif response.status_code >= 400:
            raise APIRequestError(
                f"API request failed with status {response.status_code}: {response.reason}"
            )

        return response.content

    except requests.exceptions.Timeout:
        raise APIRequestError(f"Request timed out after {DEFAULT_TIMEOUT}s for document {url} with headers {headers}")
    except requests.exceptions.ConnectionError as e:
        raise APIRequestError(f"Connection error while fetching {url} with headers {headers}: {str(e)}")
    except requests.exceptions.RequestException as e:
        raise APIRequestError(f"Request failed for {url} with headers {headers}: {str(e)}")


def get_full_text_xhtml(celex_id: str, language: str = "eng", logger: logging.Logger = None) -> bytes:
    """
    Retrieve full text in XHTML format for a given CELEX ID.

    Args:
        celex_id: The CELEX identifier
        language: Language code (default: "eng")
        logger: Optional logger

    Returns:
        Document content as bytes
    """

    celex_id = validate_celex(celex_id)
    headers = {"Accept": "application/xhtml+xml", "Accept-Language": language}
    return _get_request_with_300_handling(
        _get_celex_url(celex_id),
        headers,
        include_keywords={'ACT'},
        exclude_keywords={'annexe', 'annex', 'cover', 'erratum', 'corrigendum'},
        logger=logger
    )


def get_full_text_plain_html(celex_id: str, language: str = "eng", logger: logging.Logger = None) -> bytes:
    """
    Retrieve full text in plain HTML format for a given CELEX ID.

    Args:
        celex_id: The CELEX identifier
        language: Language code (default: "eng")
        logger: Optional logger

    Returns:
        Document content as bytes
    """

    celex_id = validate_celex(celex_id)
    headers = {"Accept": "text/html", "Accept-Language": language}
    return _get_request_with_300_handling(
        _get_celex_url(celex_id),
        headers,
        include_keywords={'ACT'},
        exclude_keywords={'annexe', 'annex', 'cover', 'erratum', 'corrigendum'},
        logger=logger
    )


def get_annex_xhtml(celex_id: str, language: str = "eng", logger: logging.Logger = None) -> bytes:
    """
    Retrieve annex in XHTML format for a given CELEX ID.

    Args:
        celex_id: The CELEX identifier
        language: Language code (default: "eng")
        logger: Optional logger

    Returns:
        Annex document content as bytes
    """

    celex_id = validate_celex(celex_id)
    headers = {"Accept": "application/xhtml+xml", "Accept-Language": language}
    return _get_request_with_300_handling(
        _get_celex_url(celex_id),
        headers,
        include_keywords={'annex', 'annexe'},
        exclude_keywords={'ACT', 'cover', 'erratum', 'corrigendum'},
        logger=logger
    )


def get_annex_plain_html(celex_id: str, language: str = "eng", logger: logging.Logger = None) -> bytes:
    """
    Retrieve annex in plain HTML format for a given CELEX ID.

    Args:
        celex_id: The CELEX identifier
        language: Language code (default: "eng")
        logger: Optional logger

    Returns:
        Annex document content as bytes
    """

    celex_id = validate_celex(celex_id)
    headers = {"Accept": "text/html", "Accept-Language": language}
    return _get_request_with_300_handling(
        _get_celex_url(celex_id),
        headers,
        include_keywords={'annex', 'annexe'},
        exclude_keywords={'ACT', 'cover', 'erratum', 'corrigendum'},
        logger=logger
    )


def get_work_metadata_xml(celex_id: str, language: str = "eng", logger: logging.Logger = None) -> bytes:
    celex_id = validate_celex(celex_id)
    headers = {"Accept": "application/xml;notice=object"}
    return get_request(_get_celex_url(celex_id), headers, logger)


def get_expression_metadata_xml(celex_id: str, language: str = "eng", logger: logging.Logger = None) -> bytes:
    celex_id = validate_celex(celex_id)
    headers = {"Accept": "application/xml;notice=object", "Accept-Language": language}
    return get_request(_get_celex_url(celex_id), headers, logger)
