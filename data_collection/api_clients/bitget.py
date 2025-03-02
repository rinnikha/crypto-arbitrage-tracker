# data_collection/api_clients/bitget.py
import time
import base64
import hmac
import hashlib
import requests
from urllib.parse import urlencode

from core.dto import PricePointDTO
from core.models import PricePoint
from data_collection.base import BaseCollector
from core.utils import make_request

class BitgetCollector(BaseCollector):
    """Collector for Bitget exchange data."""
    
    def __init__(self, api_key=None, api_secret=None, passphrase=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.base_url = "https://api.bitget.com"
        self.p2p_url = "https://api.bitget.com/api/spot/v1/p2p/merchant/advertise/list"
    
    def _generate_signature(self, timestamp, method, request_path, body=''):
        """Generate signature for Bitget API requests."""
        message = str(timestamp) + method.upper() + request_path
        if body:
            message += body
        
        signature = base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')
        
        return signature
    
    def _get_headers(self, method, endpoint, body=''):
        """Get headers for Bitget API requests."""
        timestamp = str(int(time.time() * 1000))
        signature = self._generate_signature(timestamp, method, endpoint, body)
        
        headers = {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json'
        }
        
        return headers
    
    def fetch_p2p_prices(self, asset):
        """Fetch P2P prices from Bitget."""
        price_points = []
        
        # For buy orders
        buy_params = {
            "coinId": asset,
            "currencyId": "USD",
            "side": "buy",
            "pageSize": 10,
            "pageNo": 1
        }
        
        buy_response = make_request(
            url=self.p2p_url,
            method="GET",
            params=buy_params
        )
        
        buy_data = buy_response.json()
        
        # Process buy orders
        if buy_data.get('code') == '00000' and 'data' in buy_data:
            for adv in buy_data['data'].get('advList', []):
                price_point = PricePointDTO(
                    exchange_name="Bitget",
                    asset_symbol=asset,
                    price=float(adv.get('price', 0)),
                    order_type="BUY",
                    market_type="P2P",
                    available_amount=float(adv.get('surplusAmount', 0)),
                    min_amount=float(adv.get('minAmount', 0)),
                    max_amount=float(adv.get('maxAmount', 0)),
                    payment_methods=adv.get('payMethods', [])
                )
                price_points.append(price_point)
        
        # For sell orders
        sell_params = {
            "coinId": asset,
            "currencyId": "USD",
            "side": "sell",
            "pageSize": 10,
            "pageNo": 1
        }
        
        sell_response = make_request(
            url=self.p2p_url,
            method="GET",
            params=sell_params
        )
        
        sell_data = sell_response.json()
        
        # Process sell orders
        if sell_data.get('code') == '00000' and 'data' in sell_data:
            for adv in sell_data['data'].get('advList', []):
                price_point = PricePointDTO(
                    exchange_name="Bitget",
                    asset_symbol=asset,
                    price=float(adv.get('price', 0)),
                    order_type="SELL",
                    market_type="P2P",
                    available_amount=float(adv.get('surplusAmount', 0)),
                    min_amount=float(adv.get('minAmount', 0)),
                    max_amount=float(adv.get('maxAmount', 0)),
                    payment_methods=adv.get('payMethods', [])
                )
                price_points.append(price_point)
        
        return price_points
    
    def fetch_exchange_prices(self, asset):
        """Fetch exchange prices from Bitget."""
        endpoint = "/api/spot/v1/market/ticker"
        params = {
            "symbol": f"{asset}USDT"
        }
        
        response = make_request(
            url=f"{self.base_url}{endpoint}",
            method="GET",
            params=params
        )
        
        data = response.json()
        
        if data.get('code') == '00000' and 'data' in data:
            ticker = data['data']
            return PricePoint(
                exchange="Bitget",
                asset=asset,
                price=float(ticker.get('close', 0)),
                order_type="MARKET",
                market_type="EXCHANGE"
            )
        
        return None
    
    def fetch_available_amount(self, asset, order_type):
        """Fetch available amount from Bitget order book."""
        endpoint = "/api/spot/v1/market/depth"
        params = {
            "symbol": f"{asset}USDT",
            "limit": 5,
            "type": "step0"
        }
        
        response = make_request(
            url=f"{self.base_url}{endpoint}",
            method="GET",
            params=params
        )
        
        data = response.json()
        
        total_amount = 0
        if data.get('code') == '00000' and 'data' in data:
            if order_type == "BUY":
                # If buying crypto, look at ask orders
                for price, amount in data['data'].get('asks', []):
                    total_amount += float(amount)
            else:
                # If selling crypto, look at bid orders
                for price, amount in data['data'].get('bids', []):
                    total_amount += float(amount)
        
        return total_amount