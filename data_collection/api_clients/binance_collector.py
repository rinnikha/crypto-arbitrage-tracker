"""
Binance data collector using the mapper framework.
"""
import logging
import time
import hmac
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlencode

from core.cache import SymbolInfoCache
from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.api_clients import  RestApiCollector
from core.utils.http import HttpClient
from core.mapping import get_mapper_registry

# Ensure mappers are initialized
import data_collection.api_clients.mappers

logger = logging.getLogger(__name__)


class BinanceCollector(RestApiCollector):
    """Collector for Binance exchange data using the mapper framework."""

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """Initialize the Binance collector."""
        super().__init__(
            exchange_name="Binance",
            base_url="https://api.binance.com",
            api_key=api_key,
            api_secret=api_secret
        )

        # P2P API has a different base URL
        self.p2p_client = HttpClient(
            base_url="https://p2p.binance.com",
            default_headers={
                'Accept': '*/*',
                'User-Agent': 'Crypto-Arbitrage-Tracker/1.0',
                'Content-Type': 'application/json'
            }
        )

        # Get mapper registry
        self.mapper_registry = get_mapper_registry()

    def _get_auth_headers(self, method: str, endpoint: str,
                          params: Optional[Dict] = None,
                          data: Optional[Dict] = None) -> Dict[str, str]:
        """Generate authentication headers for Binance API."""
        headers = {}

        if self.api_key and self.api_secret:
            # Add API key header
            headers["X-MBX-APIKEY"] = self.api_key

            # Sign the request if needed
            if params or data:
                # Add timestamp and signature
                timestamp = int(time.time() * 1000)
                query_params = params or {}
                query_params["timestamp"] = timestamp

                # Create signature
                query_string = urlencode(query_params)
                signature = hmac.new(
                    self.api_secret.encode('utf-8'),
                    query_string.encode('utf-8'),
                    hashlib.sha256
                ).hexdigest()

                # Update params with signature
                query_params["signature"] = signature

        return headers

    def fetch_p2p_orders(self, asset: str, fiats: List[str]) -> List[P2POrderDTO]:
        """Fetch P2P orders from Binance using the mapper."""
        orders = []

        # Process buy, sell and different fiat orders
        for fiat in fiats:
            for trade_type in ["BUY", "SELL"]:
                payload = {
                    "asset": asset,
                    "fiat": fiat,
                    "page": 1,
                    "rows": 20,
                    "tradeType": trade_type
                }

                try:
                    response = self.p2p_client.post(
                        endpoint="/bapi/c2c/v2/friendly/c2c/adv/search",
                        json_data=payload
                    )

                    data = response.json()

                    # Prepare data for mapping
                    # for order_data in data.get('data', []):
                    #     # Add asset and trade type to the data for mapping

                    # Use the mapper to convert to DTOs
                    new_orders = self.mapper_registry.map_many(
                        "binance_p2p_order",
                        data.get('data', [])
                    )

                    orders.extend(new_orders)

                except Exception as e:
                    logger.error(f"Error fetching Binance {trade_type} P2P orders {asset} - {fiat}: {e}")

        return orders

    def fetch_spot_pairs(self, base_asset: Optional[str] = None,
                         quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot market pairs from Binance using the mapper."""
        try:
            # Get symbol info from cache
            symbol_cache = SymbolInfoCache.get_instance()

            # Get ticker data
            ticker_data = self._fetch_ticker_data()

            # Prepare data for mapping
            pairs_data = []
            for ticker in ticker_data:
                symbol = ticker.get('symbol', '')

                # Get symbol info from cache
                symbol_info = symbol_cache.get_symbol_info("binance", symbol)
                base = symbol_info.get('base_asset', '')
                quote = symbol_info.get('quote_asset', '')

                # Filter by base/quote asset if provided
                if base_asset and base != base_asset:
                    continue

                if quote_asset and quote != quote_asset:
                    continue

                if base and quote:
                    ticker['baseAsset'] = base
                    ticker['quoteAsset'] = quote

                    # Create combined data for the mapper
                    pairs_data.append(ticker)
                else:
                    logger.warning(f"Symbol {symbol} not found in Binance Symbol's info")

            # Use the mapper to convert to DTOs
            return self.mapper_registry.map_many("binance_spot_pair", pairs_data)

        except Exception as e:
            logger.error(f"Error fetching Binance spot pairs: {e}")
            return []

    def _fetch_ticker_data(self) -> List[Dict[str, Any]]:
        """Fetch 24-hour ticker data."""
        response = self.http_client.get("/api/v3/ticker/24hr")
        return response.json()

    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount from order book."""
        try:
            # Fetch order book depth
            params = {
                "symbol": f"{asset}USDT",
                "limit": 5  # Get top 5 orders
            }

            response = self.http_client.get("/api/v3/depth", params=params)
            data = response.json()

            # Calculate total amount from appropriate side of the book
            side_key = 'asks' if order_type == "BUY" else 'bids'

            return sum(float(amount) for price, amount in data.get(side_key, []))

        except Exception as e:
            logger.error(f"Error fetching Binance available amount for {asset}: {e}")
            return 0.0