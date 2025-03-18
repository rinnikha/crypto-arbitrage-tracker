# data_collection/base_collector.py
"""
Enhanced base collector classes with common functionality.
"""
import logging
import time
from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Type, Generic, TypeVar
import hmac
import hashlib
import json
from urllib.parse import urlencode
from dataclasses import dataclass, field

from core.utils.http import HttpClient
from core.dto import P2POrderDTO, SpotPairDTO
from config.settings import REQUEST_TIMEOUT, MAX_RETRIES

logger = logging.getLogger(__name__)

# Type variables
T = TypeVar('T')  # DTO type


class BaseCollector(ABC):
    """
    Base abstract class for all data collectors.

    This defines the common interface for collection operations.
    Specific collector types (REST API, WebSocket, etc.) should extend this.
    """

    def __init__(self, exchange_name: str, base_url: str = "", api_key: Optional[str] = None):
        """
        Initialize the collector.

        Args:
            exchange_name: Name of the exchange
            base_url: Base URL for the exchange API
            api_key: API key for authenticated requests
        """
        self.exchange_name = exchange_name
        self.base_url = base_url
        self.api_key = api_key

    @abstractmethod
    def fetch_p2p_orders(self, asset: str) -> List[P2POrderDTO]:
        """
        Fetch P2P orders for the given asset.

        Args:
            asset: Asset symbol

        Returns:
            List of P2P order DTOs
        """
        pass

    @abstractmethod
    def fetch_spot_pairs(self, base_asset: Optional[str] = None,
                         quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """
        Fetch spot market pairs.

        Args:
            base_asset: Filter by base asset (optional)
            quote_asset: Filter by quote asset (optional)

        Returns:
            List of spot pair DTOs
        """
        pass

    @abstractmethod
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """
        Fetch available amount for the given asset and order type.

        Args:
            asset: Asset symbol
            order_type: Order type (BUY, SELL)

        Returns:
            Available amount
        """
        pass

    def get_exchange_name(self) -> str:
        """Get the exchange name."""
        return self.exchange_name


class RestApiCollector(BaseCollector):
    """
    Base class for collectors that use REST APIs.

    This provides common functionality for REST API-based collectors.
    """

    def __init__(self, exchange_name: str, base_url: str, api_key: Optional[str] = None,
                 api_secret: Optional[str] = None, passphrase: Optional[str] = None):
        """
        Initialize the collector.

        Args:
            exchange_name: Name of the exchange
            base_url: Base URL for the exchange API
            api_key: API key for authenticated requests
            api_secret: API secret for authenticated requests
            passphrase: Passphrase for authenticated requests (some exchanges)
        """
        super().__init__(exchange_name, base_url, api_key)
        self.api_secret = api_secret
        self.passphrase = passphrase

        # Create HTTP client
        self.http_client = HttpClient(
            base_url=base_url,
            default_headers=self._get_default_headers(),
            timeout=REQUEST_TIMEOUT,
            max_retries=MAX_RETRIES
        )

    def _get_default_headers(self) -> Dict[str, str]:
        """
        Get default headers for requests.

        Returns:
            Default headers dictionary
        """
        headers = {
            "User-Agent": "Crypto-Arbitrage-Tracker/1.0",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }

        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        return headers

    def _auth_request(self, method: str, endpoint: str,
                      params: Optional[Dict] = None,
                      data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make an authenticated request.

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: URL parameters
            data: Request body

        Returns:
            Response data as dictionary
        """
        # Add authentication headers
        headers = self._get_auth_headers(method, endpoint, params, data)

        # Make the request
        response = self.http_client.request(
            method=method,
            endpoint=endpoint,
            params=params,
            json_data=data,
            headers=headers
        )

        # Parse the response
        return response.json()

    def _public_request(self, method: str, endpoint: str,
                        params: Optional[Dict] = None,
                        data: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a public (unauthenticated) request.

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: URL parameters
            data: Request body

        Returns:
            Response data as dictionary
        """
        # Make the request
        response = self.http_client.request(
            method=method,
            endpoint=endpoint,
            params=params,
            json_data=data
        )

        # Parse the response
        return response.json()

    def _get_auth_headers(self, method: str, endpoint: str,
                          params: Optional[Dict] = None,
                          data: Optional[Dict] = None) -> Dict[str, str]:
        """
        Get authentication headers for a request.

        This is a basic implementation. Subclasses should override this
        to implement exchange-specific authentication.

        Args:
            method: HTTP method
            endpoint: API endpoint
            params: URL parameters
            data: Request body

        Returns:
            Authentication headers
        """
        # Base implementation with common pattern
        headers = {}

        if self.api_key and self.api_secret:
            # Current timestamp in milliseconds
            timestamp = str(int(time.time() * 1000))

            # Create signature string (common pattern, override as needed)
            query_string = ""
            if params:
                query_string = urlencode(params, doseq=True)

            payload = f"{timestamp}{method.upper()}{endpoint}{query_string}"
            if data:
                payload += json.dumps(data)

            # Create HMAC SHA256 signature
            signature = hmac.new(
                self.api_secret.encode('utf-8'),
                payload.encode('utf-8'),
                hashlib.sha256
            ).hexdigest()

            # Add headers
            headers["API-Key"] = self.api_key
            headers["API-Timestamp"] = timestamp
            headers["API-Signature"] = signature

            # Add passphrase if provided
            if self.passphrase:
                headers["API-Passphrase"] = self.passphrase

        return headers


class WebScraperCollector(BaseCollector):
    """
    Base class for collectors that use web scraping.

    This provides common functionality for web scraper-based collectors.
    """

    def __init__(self, exchange_name: str, base_url: str,
                 user_agent: Optional[str] = None):
        """
        Initialize the collector.

        Args:
            exchange_name: Name of the exchange
            base_url: Base URL for the exchange website
            user_agent: User agent for requests
        """
        super().__init__(exchange_name, base_url)

        # Set up user agent
        self.user_agent = user_agent or (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/91.0.4472.124 Safari/537.36"
        )

        # Create HTTP client with appropriate headers
        self.http_client = HttpClient(
            base_url=base_url,
            default_headers={
                "User-Agent": self.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive"
            },
            timeout=REQUEST_TIMEOUT * 2,  # Longer timeout for web scraping
            max_retries=MAX_RETRIES
        )

    def _get_html(self, url: str, params: Optional[Dict] = None) -> str:
        """
        Get HTML content from a URL.

        Args:
            url: URL to request
            params: URL parameters

        Returns:
            HTML content
        """
        response = self.http_client.get(url, params=params)
        return response.text

    def _get_json(self, url: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Get JSON content from a URL.

        Args:
            url: URL to request
            params: URL parameters

        Returns:
            JSON content as dictionary
        """
        response = self.http_client.get(
            url,
            params=params,
            headers={"Accept": "application/json"}
        )
        return response.json()