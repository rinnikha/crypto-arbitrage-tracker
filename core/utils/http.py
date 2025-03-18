# core/http.py
"""
Enhanced HTTP client with standardized retry logic, error handling, and logging.
"""
import logging
import time
from typing import Dict, Any, Optional, Union, Callable
import requests
from functools import wraps

from config.settings import REQUEST_TIMEOUT, MAX_RETRIES, USER_AGENT

logger = logging.getLogger(__name__)


class HttpClientError(Exception):
    """Base exception for HTTP client errors."""

    def __init__(self, message: str, status_code: Optional[int] = None,
                 response: Optional[requests.Response] = None):
        self.message = message
        self.status_code = status_code
        self.response = response
        super().__init__(self.message)


class HttpClient:
    """Enhanced HTTP client with retry logic and standardized error handling."""

    def __init__(self, base_url: str = "", default_headers: Optional[Dict[str, str]] = None,
                 timeout: int = REQUEST_TIMEOUT, max_retries: int = MAX_RETRIES):
        """
        Initialize the HTTP client.

        Args:
            base_url: Base URL for all requests
            default_headers: Default headers to include in all requests
            timeout: Default request timeout
            max_retries: Maximum number of retry attempts
        """
        self.base_url = base_url
        self.default_headers = default_headers or {
            "User-Agent": USER_AGENT,
            "Accept": "application/json"
        }
        self.timeout = timeout
        self.max_retries = max_retries
        self.session = requests.Session()
        self.session.headers.update(self.default_headers)

    def request(self, method: str, endpoint: str,
                params: Optional[Dict] = None,
                data: Optional[Dict] = None,
                json_data: Optional[Dict] = None,
                headers: Optional[Dict] = None,
                timeout: Optional[int] = None,
                retry_on: Optional[Callable[[requests.Response], bool]] = None) -> requests.Response:
        """
        Make an HTTP request with automatic retries and error handling.

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: URL endpoint (will be appended to base_url if relative)
            params: URL parameters
            data: Form data
            json_data: JSON data
            headers: Additional headers
            timeout: Request timeout (overrides instance default)
            retry_on: Custom function to determine if a retry should be attempted

        Returns:
            Response object

        Raises:
            HttpClientError: On request failure after retries
        """
        url = endpoint if endpoint.startswith(('http://', 'https://')) else f"{self.base_url}{endpoint}"
        timeout = timeout or self.timeout

        # Prepare headers
        request_headers = self.default_headers.copy()
        if headers:
            request_headers.update(headers)

        retries = 0
        last_error = None

        while retries <= self.max_retries:
            try:
                response = self.session.request(
                    method=method.upper(),
                    url=url,
                    params=params,
                    data=data,
                    json=json_data,
                    headers=request_headers,
                    timeout=timeout
                )

                # Check if we should retry based on response
                if retry_on and retry_on(response):
                    raise HttpClientError(f"Retry condition met for {url}",
                                          status_code=response.status_code,
                                          response=response)

                # Raise for status codes
                response.raise_for_status()
                return response

            except (requests.RequestException, HttpClientError) as e:
                last_error = e
                retries += 1

                if retries > self.max_retries:
                    break

                # Calculate backoff
                wait_time = 0.5 * (2 ** (retries - 1))  # Exponential backoff

                logger.warning(
                    f"Retry {retries}/{self.max_retries} for {url} after {wait_time:.2f}s "
                    f"due to {str(e)}"
                )

                time.sleep(wait_time)

        # If we get here, all retries failed
        status_code = None
        response_obj = None

        if isinstance(last_error, requests.HTTPError):
            status_code = last_error.response.status_code
            response_obj = last_error.response
        elif isinstance(last_error, HttpClientError):
            status_code = last_error.status_code
            response_obj = last_error.response

        error_msg = f"Request failed after {self.max_retries} retries: {url}"
        raise HttpClientError(error_msg, status_code=status_code, response=response_obj)

    def get(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a GET request."""
        return self.request("GET", endpoint, **kwargs)

    def post(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a POST request."""
        return self.request("POST", endpoint, **kwargs)

    def put(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a PUT request."""
        return self.request("PUT", endpoint, **kwargs)

    def delete(self, endpoint: str, **kwargs) -> requests.Response:
        """Make a DELETE request."""
        return self.request("DELETE", endpoint, **kwargs)

    def close(self):
        """Close the session."""
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


def retry_on_failure(max_retries=3, backoff_factor=0.5,
                     exceptions=(requests.RequestException,),
                     retry_on_status_codes=(429, 500, 502, 503, 504)):
    """
    Decorator for retrying a function with exponential backoff on exceptions.
    This is a lightweight alternative to using the full HttpClient.

    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Backoff multiplier
        exceptions: Tuple of exceptions to retry on
        retry_on_status_codes: Status codes to retry on
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    response = func(*args, **kwargs)

                    # Handle HTTP responses specifically
                    if isinstance(response, requests.Response):
                        if response.status_code in retry_on_status_codes:
                            raise requests.HTTPError(f"Got status code {response.status_code}")

                    return response

                except exceptions as e:
                    wait_time = backoff_factor * (2 ** retries)
                    logger.warning(
                        f"Retry {retries}/{max_retries} for {func.__name__} after {wait_time:.2f}s due to {e}"
                    )
                    retries += 1
                    if retries > max_retries:
                        raise
                    time.sleep(wait_time)

        return wrapper

    return decorator


# Simplified version of make_request for backward compatibility
@retry_on_failure(max_retries=MAX_RETRIES)
def make_request(url: str, method: str = "GET", params: Optional[Dict] = None,
                 data: Optional[Dict] = None, headers: Optional[Dict] = None,
                 timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """
    Legacy function for making HTTP requests with retry logic.
    For new code, prefer using the HttpClient class.
    """
    default_headers = {
        "User-Agent": USER_AGENT
    }

    if headers:
        default_headers.update(headers)

    response = requests.request(
        method=method,
        url=url,
        params=params,
        json=data if method.upper() != "GET" else None,
        headers=default_headers,
        timeout=timeout
    )
    response.raise_for_status()
    return response