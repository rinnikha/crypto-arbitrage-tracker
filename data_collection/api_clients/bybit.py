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
        self.p2p_url = "https://api2.bybit.com/fiat/otc/item/online"
    
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

        for side in ['sell', 'buy']:
            payload= {
                # "userId": 26473217,
                "tokenId": asset,
                "currencyId": "USD",
                "payment": [],
                "side": "0" if side == 'sell' else "1",
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
                response = make_request(
                    url=self.p2p_url,
                    method="POST",
                    data=payload,
                )

                if response.json().get('ret_code') != 0:
                    print(f"API Error: {response.json().get('msg')}")
                    continue

                for adv in response.json()['result'].get('items', []):
                    orders.append(self._create_order_dto(adv, side))
        
            except Exception as e:
                print(f"Error fetching {side} orders: {str(e)}")
    
        return orders
    
    def _create_order_dto(self, adv, side):
        return P2POrderDTO(
            exchange_name="Bybit",
            asset_symbol=adv['tokenId'].upper(),
            fiat_code=adv['currencyId'].upper(),
            price=float(adv.get('price', 0)),
            order_type="BUY" if side == 'sell' else 'SELL',
            available_amount=float(adv.get('quantity', 0)),
            min_amount=float(adv.get('minAmount', 0)),
            max_amount=float(adv.get('maxAmount', 0)),
            payment_methods=adv['payments'],
            # New fields
            order_id=adv.get('id'),
            user_id=adv.get('userId'),
            user_name=adv.get('nickName', 'Unknown'),
            completion_rate=float(adv.get('recentExecuteRate', 0))  # Convert to percentage
        )
    
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

                    if symbol == '':
                        continue
                    
                    # Attempt to split symbol into base and quote assets
                    # This is a simplistic approach - Bybit symbols may need special handling
                    base_asset_symbol = None
                    quote_asset_symbol = None
                    
                    for quote in ["USDT", "USDC", "BTC", "ETH"]:
                        if symbol.endswith(quote):
                            quote_asset_symbol = quote
                            base_asset_symbol = symbol[:-len(quote)]
                            break
                    
                    # Create the pair DTO
                    pair = SpotPairDTO(
                        exchange_name="Bybit",
                        symbol=symbol,
                        price=float(ticker.get('lastPrice', 0)),
                        bid_price=float(ticker.get('bid1Price', 0)) if ticker.get('bid1Price') != '' else 0,
                        ask_price=float(ticker.get('ask1Price', 0)) if ticker.get('ask1Price') != '' else 0,
                        volume_24h=float(ticker.get('volume24h', 0)),
                        high_24h=float(ticker.get('highPrice24h', 0)),
                        low_24h=float(ticker.get('lowPrice24h', 0)),
                        base_asset_symbol=base_asset_symbol,
                        quote_asset_symbol=quote_asset_symbol
                    )

                    if (pair.base_asset_symbol and pair.quote_asset_symbol) and pair.price != 0:
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