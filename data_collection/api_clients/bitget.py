# data_collection/api_clients/bitget.py
import time
from datetime import datetime
import base64
import uuid
import hmac
import json
import hashlib
import requests
from urllib.parse import urlencode
from typing import List, Optional

from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.base import BaseCollector
from core.utils import retry_on_failure, make_request

orders_timestamp = int(time.time() * 1000) - (89 * 24 * 60 * 60 * 1000)

class BitgetCollector(BaseCollector):
    """Collector for Bitget exchange data."""
    
    def __init__(self, api_key=None, api_secret=None, passphrase=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.passphrase = passphrase
        self.base_url = "https://api.bitget.com"


    def _get_timestamp(self, servert_time=False):
        timestamp = int(time.time() * 1000)

        if servert_time:
            endpoint = '/api/v2/public/time'
            response = make_request(
                url=f"{self.base_url}{endpoint}",
                method="GET"
            )

            timestamp = response.json().get('data').get('serverTime')

        return timestamp
        
    def sort_params(self, params: dict) -> dict:
        """
        Sorts the parameters in ascending alphabetical order by their keys.
        
        Args:
            params (dict): Dictionary of parameters to sort.
        
        Returns:
            dict: Sorted dictionary of parameters.
        """
        return dict(sorted(params.items(), key=lambda item: item[0]))
    
    def _generate_signature(self, timestamp, method, request_path, params='', body=''):
        """Generate HMAC SHA256 signature"""
        # Create canonical query string
        sorted_params = urlencode(self.sort_params(params or {}))
        message = f"{timestamp}{method.upper()}{request_path}?{sorted_params}"
        
        if body:
            message += json.dumps(body, separators=(',', ':'))  # Compact JSON
        
        return base64.b64encode(
            hmac.new(
                self.api_secret.encode('utf-8'),
                message.encode('utf-8'),
                hashlib.sha256
            ).digest()
        ).decode('utf-8')

    
    def _get_headers(self, method, request_path, params=None):
        timestamp = str(self._get_timestamp())
        signature = self._generate_signature(timestamp, method, request_path, params)
        
        return {
            'ACCESS-KEY': self.api_key,
            'ACCESS-SIGN': signature,
            'ACCESS-TIMESTAMP': timestamp,
            'ACCESS-PASSPHRASE': self.passphrase,
            'Content-Type': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
    
    @retry_on_failure(max_retries=3)
    def fetch_p2p_orders(self, asset: str) -> List[P2POrderDTO]:
        endpoint = "/api/v2/p2p/advList"
        orders = []
        
        for side in ["sell", "buy"]:  # sell=merchant sells (user buys), buy=merchant buys (user sells)
            params = {
                "coin": "USDT",
                "fiat": "RUB",
                "side": side,
                "status": "online",
                "orderBy": "price",
                "sourceType": "competitior"
            }
            
            try:
                response = make_request(
                    url=f"{self.base_url}{endpoint}",
                    method="GET",
                    params=self.sort_params(params),
                    headers=self._get_headers("GET", endpoint, params)
                )

                data = response.json()
                
                if response.json().get('code') != '00000':
                    print(f"API Error: {response.json().get('msg')}")
                    continue

                advCount = len(response.json()['data'].get('advList', []))
                    
                for adv in response.json()['data'].get('advList', []):
                    orders.append(self._create_order_dto(adv, side))
                    
            except Exception as e:
                print(f"Error fetching {side} orders: {str(e)}")
        
        return orders

    def _create_order_dto(self, adv, side):
        return P2POrderDTO(
            exchange_name="Bitget",
            asset_symbol=adv['coin'].upper(),
            fiat_code=adv['fiat'].upper(),
            price=float(adv['price']),
            order_type="BUY" if side == "sell" else "SELL",
            available_amount=float(adv['advSize']),
            min_amount=float(adv['minTradeAmount']),
            max_amount=float(adv['maxTradeAmount']),
            payment_methods=[pm['paymentMethod'] for pm in adv['paymentMethodList']],
            order_id=adv['advNo'],
            user_id='',
            user_name='',
            completion_rate=float(adv['turnoverRate']) * 100
        )
    
    @retry_on_failure(max_retries=3)
    def fetch_spot_pairs(self, base_asset: Optional[str] = None, 
                        quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot market pairs from Bitget."""
        # First get symbols info to understand the available pairs
        symbols_endpoint = "/api/v2/spot/public/symbols"
        
        try:
            symbols_response = make_request(
                url=f"{self.base_url}{symbols_endpoint}",
                method="GET"
            )
            
            symbols_data = symbols_response.json()
            symbols_map = {}
            
            if symbols_data.get('code') == '00000' and 'data' in symbols_data:
                for symbol_info in symbols_data['data']:
                    symbol = symbol_info.get('symbol')
                    base_coin = symbol_info.get('baseCoin')
                    quote_coin = symbol_info.get('quoteCoin')
                    
                    symbols_map[symbol] = {
                        'base_asset': base_coin,
                        'quote_asset': quote_coin
                    }
        except Exception as e:
            print(f"Error fetching Bitget symbols info: {e}")
            symbols_map = {}
        
        # Now get tickers for all symbols
        tickers_endpoint = "/api/v2/spot/market/tickers"
        
        try:
            tickers_response = make_request(
                url=f"{self.base_url}{tickers_endpoint}",
                method="GET"
            )
            
            tickers_data = tickers_response.json()
            pairs = []
            
            if tickers_data.get('code') == '00000' and 'data' in tickers_data:
                for ticker in tickers_data['data']:
                    symbol = ticker.get('symbol')
                    
                    # Extract base and quote asset from symbol or use from symbols_map
                    symbol_info = symbols_map.get(symbol, {})
                    base_asset_symbol = symbol_info.get('base_asset')
                    quote_asset_symbol = symbol_info.get('quote_asset')
                    
                    # Create the pair DTO
                    pair = SpotPairDTO(
                        exchange_name="Bitget",
                        symbol=symbol,
                        price=float(ticker.get('lastPr', 0)),
                        bid_price=float(ticker.get('bidPr', 0)),
                        ask_price=float(ticker.get('askPr', 0)),
                        volume_24h=float(ticker.get('baseVolume', 0)),
                        high_24h=float(ticker.get('high24h', 0)),
                        low_24h=float(ticker.get('low24h', 0)),
                        base_asset_symbol=base_asset_symbol,
                        quote_asset_symbol=quote_asset_symbol
                    )

                    if (pair.base_asset_symbol and pair.quote_asset_symbol) and pair.price != 0:
                        pairs.append(pair)

        except Exception as e:
            print(f"Error fetching Bitget spot pairs: {e}")
            pairs = []
        
        return pairs
    
    @retry_on_failure(max_retries=3)
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount from Bitget order book."""
        endpoint = "/api/v2/spot/market/orderbook"
        params = {
            "symbol": f"{asset}USDT",
            "type": "step0",
            "limit": 5
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