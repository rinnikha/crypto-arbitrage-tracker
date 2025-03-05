# data_collection/api_clients/bybit.py
import time
import hmac
import hashlib
import requests
from urllib.parse import urlencode
from typing import List, Optional

from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.base import BaseCollector
from core.utils import retry_on_failure, make_request

class BybitCollector(BaseCollector):
    """Collector for Bybit exchange data."""
    
    def __init__(self, api_key=None, api_secret=None):
        self.api_key = api_key
        self.api_secret = api_secret
        self.base_url = "https://api.bybit.com"
        self.p2p_url = "https://api.bybit.com/v5/spot/c2c/advertisement/list"
    
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
    
    @retry_on_failure(max_retries=3)
    def fetch_p2p_orders(self, asset: str) -> List[P2POrderDTO]:
        """Fetch P2P orders from Bybit."""
        orders = []
        
        # For buy orders
        buy_params = {
            "tokenId": asset,
            "currencyId": "USD",
            "side": "1",  # 1 for buy, 2 for sell
            "page": 1,
            "size": 20
        }
        
        try:
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
                    user = adv.get('advertiserInfo', {})
                    
                    order = P2POrderDTO(
                        exchange_name="Bybit",
                        asset_symbol=asset,
                        price=float(adv.get('price', 0)),
                        order_type="BUY",
                        available_amount=float(adv.get('quantity', 0)),
                        min_amount=float(adv.get('minAmount', 0)),
                        max_amount=float(adv.get('maxAmount', 0)),
                        payment_methods=[pm.get('identifier') for pm in adv.get('payments', [])],
                        # New fields
                        order_id=adv.get('advertisementId'),
                        user_id=user.get('userId'),
                        user_name=user.get('nickName', 'Unknown'),
                        completion_rate=float(user.get('completionRate', 0)) * 100  # Convert to percentage
                    )
                    orders.append(order)
        except Exception as e:
            print(f"Error fetching Bybit BUY orders: {e}")
        
        # For sell orders
        sell_params = {
            "tokenId": asset,
            "currencyId": "USD",
            "side": "2",  # 1 for buy, 2 for sell
            "page": 1,
            "size": 20
        }
        
        try:
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
                    user = adv.get('advertiserInfo', {})
                    
                    order = P2POrderDTO(
                        exchange_name="Bybit",
                        asset_symbol=asset,
                        price=float(adv.get('price', 0)),
                        order_type="SELL",
                        available_amount=float(adv.get('quantity', 0)),
                        min_amount=float(adv.get('minAmount', 0)),
                        max_amount=float(adv.get('maxAmount', 0)),
                        payment_methods=[pm.get('identifier') for pm in adv.get('payments', [])],
                        # New fields
                        order_id=adv.get('advertisementId'),
                        user_id=user.get('userId'),
                        user_name=user.get('nickName', 'Unknown'),
                        completion_rate=float(user.get('completionRate', 0)) * 100  # Convert to percentage
                    )
                    orders.append(order)
        except Exception as e:
            print(f"Error fetching Bybit SELL orders: {e}")
        
        return orders
    
    @retry_on_failure(max_retries=3)
    def fetch_spot_pairs(self, base_asset: Optional[str] = None, 
                        quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot market pairs from Bybit."""
        endpoint = "/v5/market/tickers"
        params = {
            "category": "spot"
        }
        
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
            
            if data.get('retCode') == 0 and 'result' in data:
                tickers = data['result'].get('list', [])
                
                for ticker in tickers:
                    symbol = ticker.get('symbol', '')
                    
                    # Attempt to split symbol into base and quote assets
                    # This is a simplistic approach - Bybit symbols may need special handling
                    base_asset_symbol = None
                    quote_asset_symbol = None
                    
                    for quote in ["USDT", "USD", "BTC", "ETH"]:
                        if symbol.endswith(quote):
                            quote_asset_symbol = quote
                            base_asset_symbol = symbol[:-len(quote)]
                            break
                    
                    # Create the pair DTO
                    pair = SpotPairDTO(
                        exchange_name="Bybit",
                        symbol=symbol,
                        price=float(ticker.get('lastPrice', 0)),
                        bid_price=float(ticker.get('bid1Price', 0)),
                        ask_price=float(ticker.get('ask1Price', 0)),
                        volume_24h=float(ticker.get('volume24h', 0)),
                        high_24h=float(ticker.get('highPrice24h', 0)),
                        low_24h=float(ticker.get('lowPrice24h', 0)),
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
            print(f"Error fetching Bybit spot pairs: {e}")
            pairs = []
        
        return pairs
    
    @retry_on_failure(max_retries=3)
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
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