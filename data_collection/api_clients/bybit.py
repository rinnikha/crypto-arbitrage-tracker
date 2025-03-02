# data_collection/api_clients/bybit.py
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode

from core.models import PricePoint
from data_collection.base import BaseCollector
from core.utils import make_request

class BybitCollector(BaseCollector):
    """Collector for Bybit exchange data."""
    
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.bybit.com"
        self.p2p_url = "https://api.bybit.com/v5/spot/c2c/order-book"
    
    def _generate_signature(self, params, timestamp):
        """Generate signature for Bybit API requests."""
        param_str = timestamp + self.api_key + params
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            param_str.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def _get_headers(self, params=None):
        """Get headers for Bybit API requests."""
        timestamp = str(int(time.time() * 1000))
        
        if params:
            param_str = urlencode(params)
            signature = self._generate_signature(param_str, timestamp)
        else:
            signature = self._generate_signature("", timestamp)
        
        headers = {
            'X-BAPI-API-KEY': self.api_key,
            'X-BAPI-SIGN': signature,
            'X-BAPI-TIMESTAMP': timestamp,
            'Content-Type': 'application/json'
        }
        
        return headers
    
    def fetch_p2p_prices(self, asset):
        """Fetch P2P prices from Bybit."""
        price_points = []
        
        # For buy orders
        buy_params = {
            "tokenId": asset,
            "currencyId": "USD",
            "side": "1",  # 1 for buy, 2 for sell
            "page": 1,
            "size": 10
        }
        
        buy_headers = self._get_headers(buy_params)
        
        buy_response = make_request(
            url=self.p2p_url,
            method="GET",
            params=buy_params,
            headers=buy_headers
        )
        
        buy_data = buy_response.json()
        
        # Process buy orders
        if buy_data.get('retCode') == 0 and 'result' in buy_data:
            for adv in buy_data['result'].get('items', []):
                price_point = PricePoint(
                    exchange="Bybit",
                    asset=asset,
                    price=float(adv.get('price', 0)),
                    order_type="BUY",
                    market_type="P2P",
                    available_amount=float(adv.get('quantity', 0)),
                    min_amount=float(adv.get('minAmount', 0)),
                    max_amount=float(adv.get('maxAmount', 0)),
                    payment_methods=adv.get('payments', [])
                )
                price_points.append(price_point)
        
        # For sell orders
        sell_params = {
            "tokenId": asset,
            "currencyId": "USD",
            "side": "2",  # 1 for buy, 2 for sell
            "page": 1,
            "size": 10
        }
        
        sell_headers = self._get_headers(sell_params)
        
        sell_response = make_request(
            url=self.p2p_url,
            method="GET",
            params=sell_params,
            headers=sell_headers
        )
        
        sell_data = sell_response.json()
        
        # Process sell orders
        if sell_data.get('retCode') == 0 and 'result' in sell_data:
            for adv in sell_data['result'].get('items', []):
                price_point = PricePoint(
                    exchange="Bybit",
                    asset=asset,
                    price=float(adv.get('price', 0)),
                    order_type="SELL",
                    market_type="P2P",
                    available_amount=float(adv.get('quantity', 0)),
                    min_amount=float(adv.get('minAmount', 0)),
                    max_amount=float(adv.get('maxAmount', 0)),
                    payment_methods=adv.get('payments', [])
                )
                price_points.append(price_point)
        
        return price_points
    
    def fetch_exchange_prices(self, asset):
        """Fetch exchange prices from Bybit."""
        endpoint = "/v5/market/tickers"
        params = {
            "category": "spot",
            "symbol": f"{asset}USDT"
        }
        
        response = make_request(
            url=f"{self.base_url}{endpoint}",
            method="GET",
            params=params
        )
        
        data = response.json()
        
        if data.get('retCode') == 0 and 'result' in data:
            tickers = data['result'].get('list', [])
            if tickers:
                ticker = tickers[0]
                return PricePoint(
                    exchange="Bybit",
                    asset=asset,
                    price=float(ticker.get('lastPrice', 0)),
                    order_type="MARKET",
                    market_type="EXCHANGE"
                )
        
        return None
    
    def fetch_available_amount(self, asset, order_type):
        """Fetch available amount from Bybit order book."""
        endpoint = "/v5/market/orderbook"
        params = {
            "category": "spot",
            "symbol": f"{asset}USDT",
            "limit": 5
        }
        
        response = make_request(
            url=f"{self.base_url}{endpoint}",
            method="GET",
            params=params
        )
        
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