# data_collection/api_clients/mixc.py
import time
import hmac
import hashlib
import requests
import json
from urllib.parse import urlencode
from typing import List, Optional

from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.base import BaseCollector
from core.utils import retry_on_failure, make_request

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
    
    @retry_on_failure(max_retries=3)
    def fetch_p2p_orders(self, asset: str) -> List[P2POrderDTO]:
        """Fetch P2P orders from MEXC."""
        orders = []
        
        # For buy orders
        buy_endpoint = "/otc/ads/list"
        buy_params = {
            "coinName": asset,
            "currency": "USD",
            "tradeType": "SELL",  # User sells crypto, we buy
            "page": 1,
            "pageSize": 20
        }
        
        try:
            buy_response = make_request(
                url=f"{self.p2p_url}{buy_endpoint}",
                method="GET",
                params=buy_params
            )
            
            buy_data = buy_response.json()
            
            # Process buy orders
            if buy_data.get('code') == 200 and 'data' in buy_data:
                for adv in buy_data['data'].get('list', []):
                    merchant = adv.get('merchant', {})
                    
                    order = P2POrderDTO(
                        exchange_name="MEXC",
                        asset_symbol=asset,
                        price=float(adv.get('price', 0)),
                        order_type="BUY",
                        available_amount=float(adv.get('quantity', 0)),
                        min_amount=float(adv.get('minAmount', 0)),
                        max_amount=float(adv.get('maxAmount', 0)),
                        payment_methods=[pm.get('name') for pm in adv.get('payMethods', [])],
                        # New fields
                        order_id=adv.get('id'),
                        user_id=merchant.get('uid'),
                        user_name=merchant.get('nickName', 'MEXC User'),
                        completion_rate=float(merchant.get('finishRate', 0)) * 100  # Convert to percentage
                    )
                    orders.append(order)
        except Exception as e:
            print(f"Error fetching MEXC BUY orders: {e}")
        
        # For sell orders
        sell_endpoint = "/otc/ads/list"
        sell_params = {
            "coinName": asset,
            "currency": "USD",
            "tradeType": "BUY",  # User buys crypto, we sell
            "page": 1,
            "pageSize": 20
        }
        
        try:
            sell_response = make_request(
                url=f"{self.p2p_url}{sell_endpoint}",
                method="GET",
                params=sell_params
            )
            
            sell_data = sell_response.json()
            
            # Process sell orders
            if sell_data.get('code') == 200 and 'data' in sell_data:
                for adv in sell_data['data'].get('list', []):
                    merchant = adv.get('merchant', {})
                    
                    order = P2POrderDTO(
                        exchange_name="MEXC",
                        asset_symbol=asset,
                        price=float(adv.get('price', 0)),
                        order_type="SELL",
                        available_amount=float(adv.get('quantity', 0)),
                        min_amount=float(adv.get('minAmount', 0)),
                        max_amount=float(adv.get('maxAmount', 0)),
                        payment_methods=[pm.get('name') for pm in adv.get('payMethods', [])],
                        # New fields
                        order_id=adv.get('id'),
                        user_id=merchant.get('uid'),
                        user_name=merchant.get('nickName', 'MEXC User'),
                        completion_rate=float(merchant.get('finishRate', 0)) * 100  # Convert to percentage
                    )
                    orders.append(order)
        except Exception as e:
            print(f"Error fetching MEXC SELL orders: {e}")
        
        return orders
    
    @retry_on_failure(max_retries=3)
    def fetch_spot_pairs(self, base_asset: Optional[str] = None, 
                        quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot market pairs from MEXC."""
        # First get symbols info
        symbols_endpoint = "/api/v3/exchangeInfo"
        
        try:
            symbols_response = make_request(
                url=f"{self.base_url}{symbols_endpoint}",
                method="GET"
            )
            
            symbols_data = symbols_response.json()
            symbols_map = {}
            
            if 'symbols' in symbols_data:
                for symbol_info in symbols_data['symbols']:
                    symbol = symbol_info.get('symbol')
                    base_asset_symbol = symbol_info.get('baseAsset')
                    quote_asset_symbol = symbol_info.get('quoteAsset')
                    
                    symbols_map[symbol] = {
                        'base_asset': base_asset_symbol,
                        'quote_asset': quote_asset_symbol
                    }
        except Exception as e:
            print(f"Error fetching MEXC symbols info: {e}")
            symbols_map = {}
        
        # Now get tickers
        endpoint = "/api/v3/ticker/24hr"
        params = {}
        
        if base_asset and quote_asset:
            params["symbol"] = f"{base_asset}{quote_asset}"
        
        try:
            response = make_request(
                url=f"{self.base_url}{endpoint}",
                method="GET",
                params=params
            )
            
            data = response.json()
            pairs = []
            
            # If a single ticker was returned (not a list)
            if not isinstance(data, list):
                data = [data]
            
            for ticker in data:
                symbol = ticker.get('symbol', '')
                
                # Get base and quote assets from symbols map
                symbol_info = symbols_map.get(symbol, {})
                base_asset_symbol = symbol_info.get('base_asset')
                quote_asset_symbol = symbol_info.get('quote_asset')
                
                # If we don't have the info from symbols map, try to parse it
                if not base_asset_symbol or not quote_asset_symbol:
                    for quote in ["USDT", "USD", "BTC", "ETH"]:
                        if symbol.endswith(quote):
                            quote_asset_symbol = quote
                            base_asset_symbol = symbol[:-len(quote)]
                            break
                
                # Create spot pair DTO
                pair = SpotPairDTO(
                    exchange_name="MEXC",
                    symbol=symbol,
                    price=float(ticker.get('lastPrice', 0)),
                    bid_price=float(ticker.get('bidPrice', 0)),
                    ask_price=float(ticker.get('askPrice', 0)),
                    volume_24h=float(ticker.get('volume', 0)),
                    high_24h=float(ticker.get('highPrice', 0)),
                    low_24h=float(ticker.get('lowPrice', 0)),
                    base_asset_symbol=base_asset_symbol,
                    quote_asset_symbol=quote_asset_symbol
                )
                
                # Filter by base/quote asset if provided
                if base_asset and pair.base_asset_symbol != base_asset:
                    continue
                    
                if quote_asset and pair.quote_asset_symbol != quote_asset:
                    continue
                    
                pairs.append(pair)
        except Exception as e:
            print(f"Error fetching MEXC spot pairs: {e}")
            pairs = []
        
        return pairs
    
    @retry_on_failure(max_retries=3)
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount from MEXC order book."""
        endpoint = "/api/v3/depth"
        params = {
            "symbol": f"{asset}USDT",
            "limit": 5
        }
        
        try:
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
        except Exception as e:
            print(f"Error fetching MEXC available amount: {e}")
            total_amount = 0
        
        return total_amount