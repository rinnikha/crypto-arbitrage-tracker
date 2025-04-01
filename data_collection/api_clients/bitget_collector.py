"""
Bitget data collector using the mapper framework.
"""
import logging
import time
import hmac
import hashlib
import base64
import json
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlencode

from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.api_clients import RestApiCollector
from core.utils.http import HttpClient
from core.mapping import get_mapper_registry

# Ensure mappers are initialized
import data_collection.api_clients.mappers

logger = logging.getLogger(__name__)


class BitgetCollector(RestApiCollector):
    """Collector for Bitget exchange data using the mapper framework."""

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None, passphrase: Optional[str] = None):
        """Initialize the Bitget collector."""
        super().__init__(
            exchange_name="Bitget",
            base_url="https://api.bitget.com",
            api_key=api_key,
            api_secret=api_secret,
            passphrase=passphrase
        )

        # Get mapper registry
        self.mapper_registry = get_mapper_registry()

    def _get_auth_headers(self, method: str, endpoint: str,
                          params: Optional[Dict] = None,
                          data: Optional[Dict] = None) -> Dict[str, str]:
        """Generate authentication headers for Bitget API."""
        if not self.api_key or not self.api_secret or not self.passphrase:
            return {}

        # Current timestamp in milliseconds
        timestamp = str(int(time.time() * 1000))

        # Sort parameters if provided
        params_str = ""
        if params:
            sorted_params = dict(sorted(params.items(), key=lambda item: item[0]))
            params_str = urlencode(sorted_params)

        # Create message string
        message = f"{timestamp}{method.upper()}{endpoint}"
        if params_str:
            message += f"?{params_str}"

        # Add body if provided
        if data:
            message += json.dumps(data, separators=(',', ':'))

        # Generate signature
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')

        # Return headers
        return {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }

    def fetch_p2p_orders(self, asset: str, fiats: List[str]) -> List[P2POrderDTO]:
        """Fetch P2P orders from Bitget using the mapper."""
        orders = []
        endpoint = "/api/v2/p2p/advList"

        for fiat in fiats:
            for side in ["sell", "buy"]:  # sell=merchant sells (user buys), buy=merchant buys (user sells)
                params = {
                    "coin": asset,
                    "fiat": fiat,
                    "side": side,
                    "status": "online",
                    "orderBy": "price",
                    "sourceType": "competitior"
                }

                try:
                    # Get sorted parameters for authentication
                    sorted_params = dict(sorted(params.items(), key=lambda item: item[0]))

                    # Get authentication headers
                    headers = self._get_auth_headers("GET", endpoint, sorted_params)

                    # Make request
                    response = self.http_client.get(
                        endpoint=endpoint,
                        params=sorted_params,
                        headers=headers
                    )

                    data = response.json()

                    if data.get('code') != '00000':
                        logger.error(f"Bitget API Error: {data.get('msg')}")
                        continue

                    # Process the response data
                    adv_list = data.get('data', {}).get('advList', [])

                    if not adv_list:
                        logger.warning(f"No P2P orders found for {asset} - {fiat} from Bitget")
                        continue

                    new_orders = self.mapper_registry.map_many("bitget_p2p_order", adv_list)
                    orders.extend(new_orders)

                except Exception as e:
                    logger.error(f"Error fetching Bitget {side} P2P orders for {asset} - {fiat}: {e}")

        return orders

    def fetch_spot_pairs(self, base_asset: Optional[str] = None,
                         quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot market pairs from Bitget using the mapper framework."""
        try:
            # Get symbol information first
            from core.cache import SymbolInfoCache
            symbol_cache = SymbolInfoCache.get_instance()

            # Get tickers data
            tickers_data = self._fetch_ticker_data()

            # Combine data for mapping
            pairs_data = []
            for ticker in tickers_data:
                symbol = ticker.get('symbol', '')

                # Get symbol info from cache
                symbol_info = symbol_cache.get_symbol_info("bitget", symbol)
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

                    pairs_data.append(ticker)
                else:
                    logger.warning(f"Symbol {symbol} not found in Bitget Symbol's info")

            return self.mapper_registry.map_many("bitget_spot_pair", pairs_data)

        except Exception as e:
            logger.error(f"Error fetching Bitget spot pairs: {e}")
            return []

    def _fetch_symbols_info(self) -> Dict[str, Tuple[str, str]]:
        """Fetch and process Bitget trading symbols info."""
        endpoint = "/api/v2/spot/public/symbols"
        response = self.http_client.get(endpoint)
        data = response.json()

        # Create a mapping of symbol -> (baseAsset, quoteAsset)
        symbol_info = {}

        if data.get('code') == '00000' and 'data' in data:
            for symbol_data in data['data']:
                symbol = symbol_data.get('symbol')
                base = symbol_data.get('baseCoin')
                quote = symbol_data.get('quoteCoin')

                if symbol and base and quote:
                    symbol_info[symbol] = (base, quote)

        return symbol_info

    def _fetch_ticker_data(self) -> List[Dict[str, Any]]:
        """Fetch Bitget ticker data."""
        endpoint = "/api/v2/spot/market/tickers"
        response = self.http_client.get(endpoint)
        data = response.json()

        if data.get('code') == '00000' and 'data' in data:
            return data['data']
        return []

    def _manual_map_spot_pairs(self, pairs_data: List[Dict[str, Any]]) -> List[SpotPairDTO]:
        """Manually map ticker data to SpotPairDTO objects (fallback method)."""
        result = []

        for pair_data in pairs_data:
            ticker = pair_data['ticker']
            symbol_info = pair_data['symbol_info']
            symbol = ticker.get('symbol', '')

            if symbol in symbol_info:
                base_asset, quote_asset = symbol_info[symbol]

                pair = SpotPairDTO(
                    exchange_name="Bitget",
                    symbol=symbol,
                    price=float(ticker.get('lastPr', 0)),
                    bid_price=float(ticker.get('bidPr', 0)),
                    ask_price=float(ticker.get('askPr', 0)),
                    volume_24h=float(ticker.get('baseVolume', 0)),
                    high_24h=float(ticker.get('high24h', 0)),
                    low_24h=float(ticker.get('low24h', 0)),
                    base_asset_symbol=base_asset,
                    quote_asset_symbol=quote_asset
                )

                result.append(pair)

        return result

    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount from order book."""
        try:
            endpoint = "/api/v2/spot/market/orderbook"
            params = {
                "symbol": f"{asset}USDT",
                "type": "step0",
                "limit": 5
            }

            response = self.http_client.get(endpoint, params=params)
            data = response.json()

            if data.get('code') == '00000' and 'data' in data:
                # Calculate total amount from appropriate side of the book
                side_key = 'asks' if order_type == "BUY" else 'bids'

                return sum(float(amount) for price, amount in data['data'].get(side_key, []))

            return 0.0
        except Exception as e:
            logger.error(f"Error fetching Bitget available amount for {asset}: {e}")
            return 0.0