# data_collection/api_clients/mixc.py
import time
import hmac
import hashlib
import requests
import json
from urllib.parse import urlencode

from core.models import PricePoint
from data_collection.base import BaseCollector
from core.utils import make_request

class MixcCollector(BaseCollector):
    """Collector for MEXC (MIXC) exchange data."""
    
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.mexc.com"
        self.p2p_url = "https://otc.mexc.com/api"
    
    def _generate_signature(self, params):
        """Generate signature for MEXC API requests."""
        query_string = urlencode(params)
        
        signature = hmac.new(
            self.api_secret.encode('utf-8'),
            query_string.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()
        
        return signature
    
    def _get_headers(self, params=None):
        """Get headers for MEXC API requests."""
        headers = {
            'Content-Type': 'application/json',
            'X-MEXC-APIKEY': self.api_key
        }
        
        if params:
            headers['X-MEXC-SIGNATURE'] = self._generate_signature(params)
        
        return headers
    
    def fetch_p2p_prices(self, asset):
        """Fetch P2P prices from MEXC."""
        price_points = []
        
        # For buy orders
        buy_endpoint = "/otc/ads/list"
        buy_params = {
            "coinName": asset,
            "currency": "USD",
            "tradeType": "SELL",  # User sells crypto, we buy
            "page": 1,
            "pageSize": 10
        }
        
        buy_response = make_request(
            url=f"{self.p2p_url}{buy_endpoint}",
            method="GET",
            params=buy_params
        )
        
        buy_data = buy_response.json()
        
        # Process buy orders
        if buy_data.get('code') == 200 and 'data' in buy_data:
            for adv in buy_data['data'].get('list', []):
                price_point = PricePoint(
                    exchange="MEXC",
                    asset=asset,
                    price=float(adv.get('price', 0)),
                    order_type="BUY",
                    market_type="P2P",
                    available_amount=float(adv.get('quantity', 0)),
                    min_amount=float(adv.get('minAmount', 0)),
                    max_amount=float(adv.get('maxAmount', 0)),
                    payment_methods=adv.get('payMethods', [])
                )
                price_points.append(price_point)
        
        # For sell orders
        sell_endpoint = "/otc/ads/list"
        sell_params = {
            "coinName": asset,
            "currency": "USD",
            "tradeType": "BUY",  # User buys crypto, we sell
            "page": 1,
            "pageSize": 10
        }
        
        sell_response = make_request(
            url=f"{self.p2p_url}{sell_endpoint}",
            method="GET",
            params=sell_params
        )
        
        sell_data = sell_response.json()
        
        # Process sell orders
        if sell_data.get('code') == 200 and 'data' in sell_data:
            for adv in sell_data['data'].get('list', []):
                price_point = PricePoint(
                    exchange="MEXC",
                    asset=asset,
                    price=float(adv.get('price', 0)),
                    order_type="SELL",
                    market_type="P2P",
                    available_amount=float(adv.get('quantity', 0)),
                    min_amount=float(adv.get('minAmount', 0)),
                    max_amount=float(adv.get('maxAmount', 0)),
                    payment_methods=adv.get('payMethods', [])
                )
                price_points.append(price_point)
        
        return price_points
    
    def fetch_exchange_prices(self, asset):
        """Fetch exchange prices from MEXC."""
        endpoint = "/api/v3/ticker/price"
        params = {
            "symbol": f"{asset}USDT"
        }
        
        response = make_request(
            url=f"{self.base_url}{endpoint}",
            method="GET",
            params=params
        )
        
        data = response.json()
        
        if data and 'price' in data:
            return PricePoint(
                exchange="MEXC",
                asset=asset,
                price=float(data.get('price', 0)),
                order_type="MARKET",
                market_type="EXCHANGE"
            )
        
        return None
    
    def fetch_available_amount(self, asset, order_type):
        """Fetch available amount from MEXC order book."""
        endpoint = "/api/v3/depth"
        params = {
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
        if 'asks' in data and 'bids' in data:
            if order_type == "BUY":
                # If buying crypto, look at ask orders
                for price, amount in data.get('asks', []):
                    total_amount += float(amount)
            else:
                # If selling crypto, look at bid orders
                for price, amount in data.get('bids', []):
                    total_amount += float(amount)
        
        return total_amount