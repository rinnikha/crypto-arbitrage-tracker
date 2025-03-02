import requests
import hmac
import hashlib
import time
from urllib.parse import urlencode

from core.dto import PricePointDTO
from core.models import PricePoint
from data_collection.base import BaseCollector

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
    
    def fetch_p2p_prices(self, asset):
        """Fetch P2P prices for the given asset."""
        headers = {
            'Accept': '*/*',
            'User-Agent': 'Mozilla/5.0',
            'Content-Type': 'application/json'
        }
        
        # For buy orders (buying crypto, selling fiat)
        buy_payload = {
            "asset": asset,
            "fiat": "USD",  # This could be configurable
            "page": 1,
            "rows": 10,
            "tradeType": "BUY"
        }
        
        # For sell orders (selling crypto, buying fiat)
        sell_payload = {
            "asset": asset,
            "fiat": "USD",
            "page": 1,
            "rows": 10,
            "tradeType": "SELL"
        }
        
        buy_response = requests.post(self.p2p_url, headers=headers, json=buy_payload)
        sell_response = requests.post(self.p2p_url, headers=headers, json=sell_payload)
        
        buy_data = buy_response.json()
        sell_data = sell_response.json()
        
        price_points = []
        
        # Process buy orders
        for order in buy_data.get('data', []):
            adv = order.get('adv')
            price_point = PricePointDTO(
                exchange_name="Binance",
                asset_symbol=asset,
                price=float(adv.get('price')),
                order_type="BUY",
                market_type="P2P",
                available_amount=float(adv.get('tradableQuantity', 0)),
                min_amount=float(adv.get('minSingleTransAmount', 0)),
                max_amount=float(adv.get('maxSingleTransAmount', 0)),
                payment_methods=[pm.get('payType') for pm in adv.get('tradeMethods', [])]
            )
            price_points.append(price_point)
        
        # Process sell orders
        for order in sell_data.get('data', []):
            adv = order.get('adv')
            price_point = PricePointDTO(
                exchange_name="Binance",
                asset_symbol=asset,
                price=float(adv.get('price')),
                order_type="SELL",
                market_type="P2P",
                available_amount=float(adv.get('tradableQuantity', 0)),
                min_amount=float(adv.get('minSingleTransAmount', 0)),
                max_amount=float(adv.get('maxSingleTransAmount', 0)),
                payment_methods=[pm.get('payType') for pm in adv.get('tradeMethods', [])]
            )
            price_points.append(price_point)
        
        return price_points
    
    def fetch_exchange_prices(self, asset):
        """Fetch exchange prices for the given asset."""
        endpoint = f"/api/v3/ticker/price?symbol={asset}USDT"
        response = requests.get(f"{self.base_url}{endpoint}")
        data = response.json()
        
        return PricePoint(
            exchange="Binance",
            asset=asset,
            price=float(data.get('price', 0)),
            order_type="MARKET",
            market_type="EXCHANGE"
        )
    
    def fetch_available_amount(self, asset, order_type):
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