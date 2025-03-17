import requests
import hmac
import hashlib
import time
from urllib.parse import urlencode
from typing import List, Optional

from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.base import BaseCollector
from core.utils import retry_on_failure, make_request

class BinanceCollector(BaseCollector):
    """Collector for Binance exchange data."""

    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.binance.com"
        self.p2p_url = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"

    def _generate_signature(self, query_string):
        """Generate HMAC SHA256 signature for authenticated requests."""
        return hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
    
    @retry_on_failure(max_retries=3)
    def fetch_p2p_orders(self, asset):
        """Fetch P2P prices for the given asset."""

        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0',
            'Content-Type': 'application/json'
        }
        
        # For buy orders (buying crypto, selling fiat)
        buy_payload = {
            "asset": asset,
            "fiat": "UZS",  # This could be configurable
            "page": 1,
            "rows": 20,
            "tradeType": "SELL"
        }
        
        # For sell orders (selling crypto, buying fiat)
        sell_payload = {
            "asset": asset,
            "fiat": "UZS",
            "page": 1,
            "rows": 20,
            "tradeType": "BUY"
        }

        orders = []
        
        # Process buy orders
        buy_response = make_request(
            url=self.p2p_url, 
            method="POST",
            data=buy_payload,
            headers=headers
        )

        buy_data = buy_response.json()
        total_orders = buy_data['total']
        loaded_orders = 0
        
        # Process buy orders
        while loaded_orders < total_orders:
            for order in buy_data.get('data', []):
                adv = order.get('adv')
                advertiser = order.get('advertiser', {})

                order = P2POrderDTO(
                    exchange_name="Binance",
                    asset_symbol=asset,
                    fiat_code='USD',
                    price=float(adv.get('price')),
                    order_type="BUY",
                    available_amount=float(adv.get('tradableQuantity', 0)),
                    min_amount=float(adv.get('minSingleTransAmount', 0)),
                    max_amount=float(adv.get('maxSingleTransAmount', 0)),
                    payment_methods=[pm.get('payType') for pm in adv.get('tradeMethods', [])],

                    order_id=adv.get('advNo'),
                    user_id=advertiser.get('userNo'),
                    user_name=advertiser.get('nickName'),
                    completion_rate=float(advertiser.get('monthFinishRate', 0)) * 100
                )
                orders.append(order)
                loaded_orders += 1
        
        # Process sell orders
        sell_response = make_request(
            url=self.p2p_url,
            method="POST",
            data=sell_payload,
            headers=headers
        )

        sell_data = sell_response.json()

        total_orders = buy_data['total']
        loaded_orders = 0

        while loaded_orders < total_orders:
            for order in sell_data.get('data', []):
                adv = order.get('adv')
                advertiser = order.get('advertiser', {})

                order = P2POrderDTO(
                    exchange_name="Binance",
                    asset_symbol=asset,
                    fiat_code='USD',
                    price=float(adv.get('price')),
                    order_type="SELL",
                    available_amount=float(adv.get('tradableQuantity', 0)),
                    min_amount=float(adv.get('minSingleTransAmount', 0)),
                    max_amount=float(adv.get('maxSingleTransAmount', 0)),
                    payment_methods=[pm.get('payType') for pm in adv.get('tradeMethods', [])],

                    order_id=adv.get('advNo'),
                    user_id=advertiser.get('userNo'),
                    user_name=advertiser.get('nickName'),
                    completion_rate=float(advertiser.get('monthFinishRate', 0)) * 100
                )
                orders.append(order)
                loaded_orders += 1

        return orders
    
    @retry_on_failure(max_retries=3)
    def fetch_spot_pairs(self, base_asset: Optional[str] = None,
                         quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot market pairs from Binance."""
        endpoint = f"/api/v3/ticker/24hr"

        response = make_request(
            url=f"{self.base_url}{endpoint}",
            method="GET"
        )
        data = response.json()
        pairs = []

        for ticker in data:
            symbol = ticker.get('symbol', '')

            # if not ticker['lastPrice'] or ticker['lastPrice'] == '0':
            #     continue

            # Create spot pair DTO
            pair = SpotPairDTO(
                exchange_name="Binance",
                symbol=symbol,
                price=float(ticker.get('lastPrice', 0)),
                bid_price=float(ticker.get('bidPrice', 0)),
                ask_price=float(ticker.get('askPrice', 0)),
                volume_24h=float(ticker.get('volume', 0)),
                high_24h=float(ticker.get('highPrice', 0)),
                low_24h=float(ticker.get('lowPrice', 0))
            )

            if (pair.base_asset_symbol and pair.quote_asset_symbol) and pair.price != 0:
                pairs.append(pair)

        return pairs
    
    @retry_on_failure(max_retries=3)
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount for the given asset and order type."""
        # For exchange, check the order book depth
        endpoint = "/api/v3/depth"
        params = {
            "symbol": f"{asset}USDT",
            "limit": 5  # Get top 5 orders
        }
        
        response = requests.get(f"{self.base_url}{endpoint}", params=params)
        data = response.json()
        
        total_amount = 0
        if order_type == "BUY":
            # If buying crypto, look at ask orders
            for price, amount in data.get('asks', []):
                total_amount += float(amount)
        else:
            # If selling crypto, look at bid orders
            for price, amount in data.get('bids', []):
                total_amount += float(amount)
        
        return total_amount