"""
Bybit data collector using the mapper framework.
"""
import logging
import time
import hmac
import hashlib
from typing import List, Dict, Any, Optional, Tuple
from urllib.parse import urlencode

from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.api_clients import RestApiCollector
from core.utils.http import HttpClient
from core.mapping import get_mapper_registry

# Ensure mappers are initialized
import data_collection.api_clients.mappers

logger = logging.getLogger(__name__)


class BybitCollector(RestApiCollector):
    """Collector for Bybit exchange data using the mapper framework."""

    def __init__(self, api_key: Optional[str] = None, api_secret: Optional[str] = None):
        """Initialize the Bybit collector."""
        super().__init__(
            exchange_name="Bybit",
            base_url="https://api.bybit.com",
            api_key=api_key,
            api_secret=api_secret
        )

        # P2P API may have a different URL
        self.p2p_client = HttpClient(
            base_url="https://api2.bybit.com",
            default_headers={
                'Content-Type': 'application/json',
                'User-Agent': 'Crypto-Arbitrage-Tracker/1.0'
            }
        )

        # Get mapper registry
        self.mapper_registry = get_mapper_registry()

    def _get_auth_headers(self, method: str, endpoint: str,
                          params: Optional[Dict] = None,
                          data: Optional[Dict] = None) -> Dict[str, str]:
        """Generate authentication headers for Bybit API."""
        if not self.api_key or not self.api_secret:
            return {}

        # Get current timestamp in milliseconds
        timestamp = str(int(time.time() * 1000))

        # Create parameter string for signing
        if params:
            param_str = urlencode(sorted(params.items()))
        else:
            param_str = ""

        # Create signature string
        sign_str = timestamp + self.api_key + param_str

        # Generate HMAC SHA256 signature
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            sign_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        # Return headers
        return {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-TIMESTAMP': timestamp,
            'Content-Type': 'application/json'
        }

    def fetch_p2p_orders(self, asset: str, fiats: List[str]) -> List[P2POrderDTO]:
        """Fetch P2P orders from Bybit using the mapper."""
        orders = []
        endpoint = "/fiat/otc/item/online"

        # Get payment methods from cache
        from core.cache import PaymentMethodCache
        payments_cache =  PaymentMethodCache.get_instance()

        for fiat in fiats:
            for side in ['buy', 'sell']:  # Side from platform perspective
                payload = {
                    "tokenId": asset,
                    "currencyId": fiat,
                    "payment": [],
                    "side": "0" if side == 'buy' else "1",  # 0=sell, 1=buy from platform perspective
                    "size": "20",
                    "page": "1",
                    "amount": "",
                    "vaMaker": False,
                    "bulkMaker": False,
                    "canTrade": False,
                    "verificationFilter": 0,
                    "sortType": "TRADE_PRICE",
                    "paymentPeriod": [],
                    "itemRegion": 1
                }

                try:
                    # Make request
                    response = self.p2p_client.post(
                        endpoint=endpoint,
                        json_data=payload
                    )

                    data = response.json()

                    if data.get('ret_code') != 0:
                        logger.error(f"Bybit API Error: {data.get('ret_msg')}")
                        continue

                    items = data.get('result', {}).get('items', [])

                    # Enhance items with side info before mapping
                    for item in items:
                        item['_payments'] = payments_cache.get_payment_names(
                            "bybit",
                            item.get('payments', [])
                        )

                    new_orders = self.mapper_registry.map_many("bybit_p2p_order", items)
                    orders.extend(new_orders)

                except Exception as e:
                    logger.error(f"Error fetching Bybit {side} P2P orders for {asset}: {e}")

        return orders

    def _create_p2p_order_dto(self, item: Dict[str, Any], side: str) -> P2POrderDTO:
        """Convert raw Bybit P2P order data to DTO (fallback method)."""
        return P2POrderDTO(
            exchange_name="Bybit",
            asset_symbol=item.get('tokenId', '').upper(),
            fiat_code=item.get('currencyId', 'USD').upper(),
            price=float(item.get('price', 0)),
            order_type="BUY" if side == 'sell' else "SELL",  # Convert platform side to user side
            available_amount=float(item.get('quantity', 0)),
            min_amount=float(item.get('minAmount', 0)),
            max_amount=float(item.get('maxAmount', 0)),
            payment_methods=item.get('payments', []),
            order_id=item.get('id', ''),
            user_id=item.get('userId', ''),
            user_name=item.get('nickName', 'Unknown'),
            completion_rate=float(item.get('recentExecuteRate', 0))
        )

    def fetch_spot_pairs(self, base_asset: Optional[str] = None,
                        quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot market pairs from Bybit using the mapper."""
        try:

            # Get symbol info from cache
            from core.cache import SymbolInfoCache
            symbol_cache = SymbolInfoCache.get_instance()

            endpoint = "/v5/market/tickers"
            params = {
                "category": "spot"
            }

            response = self.http_client.get(endpoint, params=params)
            data = response.json()

            pairs_data = []
            if data.get('retCode') == 0 and 'result' in data:
                tickers = data['result'].get('list', [])

                for ticker in tickers:
                    symbol = ticker.get('symbol', '')

                    # Use binance symbols info to get base/quote assets
                    symbol_info = symbol_cache.get_symbol_info('binance', symbol)
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

            return self.mapper_registry.map_many("bybit_spot_pair", pairs_data)

        except Exception as e:
            logger.error(f"Error fetching Bybit spot pairs: {e}")
            return []

    def _create_spot_pair_dto(self, ticker: Dict[str, Any]) -> SpotPairDTO:
        """Convert raw Bybit spot pair data to DTO (fallback method)."""
        symbol = ticker.get('symbol', '')

        # Extract base and quote assets from symbol
        base_asset_symbol = None
        quote_asset_symbol = None

        for quote in ["USDT", "USDC", "BTC", "ETH"]:
            if symbol.endswith(quote):
                quote_asset_symbol = quote
                base_asset_symbol = symbol[:-len(quote)]
                break

        return SpotPairDTO(
            exchange_name="Bybit",
            symbol=symbol,
            price=float(ticker.get('lastPrice', 0)),
            bid_price=float(ticker.get('bid1Price', 0) or 0),
            ask_price=float(ticker.get('ask1Price', 0) or 0),
            volume_24h=float(ticker.get('volume24h', 0)),
            high_24h=float(ticker.get('highPrice24h', 0)),
            low_24h=float(ticker.get('lowPrice24h', 0)),
            base_asset_symbol=base_asset_symbol,
            quote_asset_symbol=quote_asset_symbol
        )

    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount from Bybit order book."""
        try:
            endpoint = "/v5/market/orderbook"
            params = {
                "category": "spot",
                "symbol": f"{asset}USDT",
                "limit": 5
            }

            response = self.http_client.get(endpoint, params=params)
            data = response.json()

            total_amount = 0
            if data.get('retCode') == 0 and 'result' in data:
                if order_type == "BUY":
                    # If buying crypto, look at ask orders
                    for price, amount in data['result'].get('a', []):
                        total_amount += float(amount)
                else:
                    # If selling crypto, look at bid orders
                    for price, amount in data['result'].get('b', []):
                        total_amount += float(amount)

            return total_amount

        except Exception as e:
            logger.error(f"Error fetching Bybit available amount for {asset}: {e}")
            return 0.0