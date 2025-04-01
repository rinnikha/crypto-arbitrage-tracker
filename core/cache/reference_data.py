"""
Caching system for exchange reference data like symbols and payment methods.
"""
import logging
import time
import threading
from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta

from core.utils.http import HttpClient

logger = logging.getLogger(__name__)


class ReferenceDataCache:
    """Base class for reference data caching."""

    def __init__(self, cache_ttl_seconds: int = 3600):
        """
        Initialize the cache.

        Args:
            cache_ttl_seconds: Time-to-live for cache entries in seconds (default: 1 hour)
        """
        self.cache = {}
        self.last_update_time = {}
        self.lock = threading.RLock()
        self.cache_ttl = cache_ttl_seconds

    def _is_cache_valid(self, key: str) -> bool:
        """Check if cache for a key is valid and not expired."""
        if key not in self.cache or key not in self.last_update_time:
            return False

        current_time = time.time()
        return current_time - self.last_update_time[key] < self.cache_ttl

    def _update_cache(self, key: str, data: Any) -> None:
        """Update cache for a key with new data."""
        with self.lock:
            self.cache[key] = data
            self.last_update_time[key] = time.time()

    def clear_cache(self) -> None:
        """Clear the entire cache."""
        with self.lock:
            self.cache.clear()
            self.last_update_time.clear()


class SymbolInfoCache(ReferenceDataCache):
    """Cache for exchange symbol information."""

    # Singleton instance
    _instance = None

    @classmethod
    def get_instance(cls) -> 'SymbolInfoCache':
        """Get the singleton instance."""
        if cls._instance is None:
            cls._instance = SymbolInfoCache()
        return cls._instance

    def get_symbol_info(self, exchange_name: str, symbol: str) -> Dict[str, str]:
        """
        Get base and quote assets for a symbol.

        Args:
            exchange_name: Name of the exchange
            symbol: Trading pair symbol

        Returns:
            Dictionary with base_asset and quote_asset
        """
        exchange_key = exchange_name.lower()

        with self.lock:
            # Check if we need to fetch/refresh the cache
            if not self._is_cache_valid(exchange_key):
                self._refresh_symbols(exchange_key)

            # Get the symbols dictionary for this exchange
            symbols_dict = self.cache.get(exchange_key, {})

            # Return the symbol info or try to parse it
            if symbol in symbols_dict:
                return symbols_dict[symbol]
            else:
                # Try to parse based on common patterns as fallback
                return self._parse_symbol(symbol)

    def get_all_symbols(self, exchange_name: str) -> Dict[str, Dict[str, str]]:
        """
        Get all symbol information for an exchange.

        Args:
            exchange_name: Name of the exchange

        Returns:
            Dictionary mapping symbols to their info
        """
        exchange_key = exchange_name.lower()

        with self.lock:
            # Check if we need to fetch/refresh the cache
            if not self._is_cache_valid(exchange_key):
                self._refresh_symbols(exchange_key)

            return self.cache.get(exchange_key, {})

    def _refresh_symbols(self, exchange_key: str) -> None:
        """
        Refresh symbol information for an exchange.

        Args:
            exchange_key: Exchange key (lowercase name)
        """
        try:
            logger.info(f"Refreshing symbol information for {exchange_key}")

            # Call the appropriate fetch method based on exchange
            if exchange_key == "binance":
                symbols = self._fetch_binance_symbols()
            elif exchange_key == "bybit":
                symbols = self._fetch_bybit_symbols()
            elif exchange_key == "bitget":
                symbols = self._fetch_bitget_symbols()
            elif exchange_key == "mexc":
                symbols = self._fetch_mexc_symbols()
            else:
                logger.warning(f"No symbol fetch method implemented for {exchange_key}")
                symbols = {}

            # Update the cache
            self._update_cache(exchange_key, symbols)
            logger.info(f"Updated symbol cache for {exchange_key} with {len(symbols)} symbols")

        except Exception as e:
            logger.error(f"Error refreshing symbol information for {exchange_key}: {e}")

    def _fetch_binance_symbols(self) -> Dict[str, Dict[str, str]]:
        """Fetch symbols from Binance."""
        client = HttpClient(base_url="https://api.binance.com")
        response = client.get("/api/v3/exchangeInfo")
        data = response.json()

        symbols = {}
        for symbol_data in data.get('symbols', []):
            symbol = symbol_data.get('symbol')
            base_asset = symbol_data.get('baseAsset')
            quote_asset = symbol_data.get('quoteAsset')

            if symbol and base_asset and quote_asset:
                symbols[symbol] = {
                    'base_asset': base_asset,
                    'quote_asset': quote_asset
                }

        return symbols

    def _fetch_bybit_symbols(self) -> Dict[str, Dict[str, str]]:
        """Fetch symbols from Bybit."""
        client = HttpClient(base_url="https://api.bybit.com")
        response = client.get("/v5/market/instruments-info", params={"category": "spot"})
        data = response.json()

        symbols = {}
        if data.get('retCode') == 0 and 'result' in data:
            for symbol_data in data['result'].get('list', []):
                symbol = symbol_data.get('symbol')
                base_asset = symbol_data.get('baseCoin')
                quote_asset = symbol_data.get('quoteCoin')

                if symbol and base_asset and quote_asset:
                    symbols[symbol] = {
                        'base_asset': base_asset,
                        'quote_asset': quote_asset
                    }

        return symbols

    def _fetch_bitget_symbols(self) -> Dict[str, Dict[str, str]]:
        """Fetch symbols from Bitget."""
        client = HttpClient(base_url="https://api.bitget.com")
        response = client.get("/api/v2/spot/public/symbols")
        data = response.json()

        symbols = {}
        if data.get('code') == '00000' and 'data' in data:
            for symbol_data in data['data']:
                symbol = symbol_data.get('symbol')
                base_asset = symbol_data.get('baseCoin')
                quote_asset = symbol_data.get('quoteCoin')

                if symbol and base_asset and quote_asset:
                    symbols[symbol] = {
                        'base_asset': base_asset,
                        'quote_asset': quote_asset
                    }

        return symbols

    def _fetch_mexc_symbols(self) -> Dict[str, Dict[str, str]]:
        """Fetch symbols from MEXC."""
        client = HttpClient(base_url="https://api.mexc.com")
        response = client.get("/api/v3/exchangeInfo")
        data = response.json()

        symbols = {}
        for symbol_data in data.get('symbols', []):
            symbol = symbol_data.get('symbol')
            base_asset = symbol_data.get('baseAsset')
            quote_asset = symbol_data.get('quoteAsset')

            if symbol and base_asset and quote_asset:
                symbols[symbol] = {
                    'base_asset': base_asset,
                    'quote_asset': quote_asset
                }

        return symbols

    def _parse_symbol(self, symbol: str) -> Dict[str, str]:
        """
        Fallback method to parse symbol when not found in cache.

        Args:
            symbol: Trading pair symbol

        Returns:
            Dictionary with base_asset and quote_asset
        """
        # Common quote assets ordered by length (longest first)
        common_quotes = ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB"]

        for quote in common_quotes:
            if symbol.endswith(quote):
                base = symbol[:-len(quote)]
                if base:  # Ensure we don't get an empty base
                    return {
                        'base_asset': base,
                        'quote_asset': quote
                    }

        # Last resort
        return {
            'base_asset': '',
            'quote_asset': ''
        }


class PaymentMethodCache(ReferenceDataCache):
    """Cache for exchange payment method information."""

    # Singleton instance
    _instance = None

    @classmethod
    def get_instance(cls) -> 'PaymentMethodCache':
        """Get the singleton instance."""
        if cls._instance is None:
            cls._instance = PaymentMethodCache()
        return cls._instance

    def get_payment_name(self, exchange_name: str, payment_id: str) -> str:
        """
        Get payment method name by ID.

        Args:
            exchange_name: Name of the exchange
            payment_id: Payment method ID

        Returns:
            Payment method name
        """
        exchange_key = exchange_name.lower()

        with self.lock:
            # Check if we need to fetch/refresh the cache
            if not self._is_cache_valid(exchange_key):
                self._refresh_payment_methods(exchange_key)

            # Get the payment methods dictionary for this exchange
            payment_methods = self.cache.get(exchange_key, {})

            # Return the payment name or a default
            return payment_methods.get(payment_id, f"Unknown-{payment_id}")

    def get_payment_names(self, exchange_name: str, payment_ids: List[str]) -> List[str]:
        """
        Convert a list of payment method IDs to their corresponding names.

        Args:
            exchange_name: Name of the exchange
            payment_ids: List of payment method IDs

        Returns:
            List of payment method names
        """
        if not payment_ids:
            return []

        exchange_key = exchange_name.lower()

        with self.lock:
            # Check if we need to fetch/refresh the cache
            if not self._is_cache_valid(exchange_key):
                self._refresh_payment_methods(exchange_key)

            # Get the payment methods dictionary for this exchange
            payment_methods = self.cache.get(exchange_key, {})

            # Convert each ID to a name
            payment_names = []
            for payment_id in payment_ids:
                # Get the name or use a default if not found
                payment_name = payment_methods.get(payment_id, f"Unknown-{payment_id}")
                payment_names.append(payment_name)

            return payment_names

    def get_all_payment_methods(self, exchange_name: str) -> Dict[str, str]:
        """
        Get all payment methods for an exchange.

        Args:
            exchange_name: Name of the exchange

        Returns:
            Dictionary mapping payment IDs to names
        """
        exchange_key = exchange_name.lower()

        with self.lock:
            # Check if we need to fetch/refresh the cache
            if not self._is_cache_valid(exchange_key):
                self._refresh_payment_methods(exchange_key)

            return self.cache.get(exchange_key, {})

    def _refresh_payment_methods(self, exchange_key: str) -> None:
        """
        Refresh payment method information for an exchange.

        Args:
            exchange_key: Exchange key (lowercase name)
        """
        try:
            logger.info(f"Refreshing payment methods for {exchange_key}")

            # Call the appropriate fetch method based on exchange
            if exchange_key == "bybit":
                payment_methods = self._fetch_bybit_payment_methods()
            elif exchange_key == "binance":
                payment_methods = self._fetch_binance_payment_methods()
            elif exchange_key == "mexc":
                payment_methods = self._fetch_mexc_payment_methods()
            else:
                logger.warning(f"No payment method fetch implemented for {exchange_key}")
                payment_methods = {}

            # Update the cache
            self._update_cache(exchange_key, payment_methods)
            logger.info(f"Updated payment method cache for {exchange_key} with {len(payment_methods)} methods")

        except Exception as e:
            logger.error(f"Error refreshing payment methods for {exchange_key}: {e}")

    def _fetch_bybit_payment_methods(self) -> Dict[str, str]:
        """Fetch payment methods from Bybit."""
        client = HttpClient(base_url="https://api2.bybit.com")
        response = client.post("/fiat/otc/configuration/queryAllPaymentList")
        data = response.json()

        payment_methods = {}
        if data.get('ret_code') == 0 and 'result' in data:
            for method in data['result'].get('paymentConfigVo', []):
                payment_id = method.get('paymentType')
                payment_name = method.get('paymentName')

                if payment_id and payment_name:
                    payment_methods[payment_id] = payment_name

        return payment_methods

    def _fetch_binance_payment_methods(self) -> Dict[str, str]:
        """Fetch payment methods from Binance."""
        # Note: This is an example - adjust the endpoint as needed
        client = HttpClient(base_url="https://p2p.binance.com")
        response = client.get("/bapi/c2c/v2/public/c2c/adv/filter-conditions")
        data = response.json()

        payment_methods = {}
        for method in data.get('data', {}).get('payMethods', []):
            payment_id = method.get('id') or method.get('identifier')
            payment_name = method.get('name')

            if payment_id and payment_name:
                payment_methods[payment_id] = payment_name

        return payment_methods

    def _fetch_mexc_payment_methods(self) -> Dict[str, str]:
        """Fetch payment methods from MEXC."""
        client = HttpClient(base_url="https://p2p.mexc.com")
        response = client.get("/api/payment/method")
        data = response.json()

        payment_methods = {}
        for method in data.get('data', []):
            payment_id = method.get('id')
            payment_name = method.get('name')

            if payment_id and payment_name:
                payment_methods[payment_id] = payment_name
        return payment_methods