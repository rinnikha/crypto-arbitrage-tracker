# data_collection/api_clients/ton_wallet.py
import requests
import json
from datetime import datetime
from typing import List, Optional

from core.dto import P2POrderDTO, SpotPairDTO
from data_collection.base import BaseCollector
from core.utils import retry_on_failure, make_request
from data_collection.scrapers.generic_scraper import GenericScraper

class TonWalletCollector(BaseCollector):
    """Collector for TON Wallet data using Telegram API and web scraping."""
    
    def __init__(self, api_token=None):
        self.api_token = api_token
        self.base_url = "https://toncenter.com/api/v2"
        self.frag_channels = [
            "@fragment_ton",
            "@fragment_ton_bot"
        ]
        self.scraper = GenericScraper(
            exchange_name="TON P2P",
            base_url="https://fragment.com",
            p2p_url="https://fragment.com/exchange/TONCOIN"
        )
    
    @retry_on_failure(max_retries=3)
    def fetch_p2p_orders(self, asset: str) -> List[P2POrderDTO]:
        """Fetch P2P orders for TON."""
        orders = []
        
        if asset != "TON":
            # Only support TON asset
            return orders
        
        # Use the scraper to get raw data from Fragment platform
        raw_orders = self.scraper.fetch_p2p_from_fragment("TON")
        
        # Convert raw scraper data to P2POrderDTO objects
        for raw_order in raw_orders:
            order = P2POrderDTO(
                exchange_name="TON P2P",
                asset_symbol="TON",
                price=float(raw_order.get('price', 0)),
                order_type=raw_order.get('order_type', 'BUY'),
                available_amount=float(raw_order.get('available_amount', 0)),
                min_amount=float(raw_order.get('min_amount', 0)) if raw_order.get('min_amount') else None,
                max_amount=float(raw_order.get('max_amount', 0)) if raw_order.get('max_amount') else None,
                payment_methods=raw_order.get('payment_methods', []),
                # New fields - fill with available data or defaults
                order_id=raw_order.get('order_id', f"fragment-{datetime.now().timestamp()}"),
                user_id=raw_order.get('user_id'),
                user_name=raw_order.get('user_name', 'Fragment User'),
                completion_rate=float(raw_order.get('completion_rate', 0)) 
            )
            orders.append(order)
        
        return orders
    
    @retry_on_failure(max_retries=3)
    def fetch_spot_pairs(self, base_asset: Optional[str] = None, 
                        quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """Fetch spot pairs for TON from external APIs."""
        pairs = []
        
        # If specific assets are requested and they're not TON, return empty
        if base_asset and base_asset != "TON":
            return pairs
        
        # Try multiple sources to get TON price data
        
        # 1. Try CoinGecko API
        try:
            response = make_request(
                url="https://api.coingecko.com/api/v3/simple/price",
                method="GET",
                params={
                    "ids": "the-open-network",
                    "vs_currencies": "usd,btc,eth"
                }
            )
            
            data = response.json()
            
            if 'the-open-network' in data:
                # Create a pair for TON/USD
                if 'usd' in data['the-open-network'] and (not quote_asset or quote_asset == "USD" or quote_asset == "USDT"):
                    ton_usd_pair = SpotPairDTO(
                        exchange_name="CoinGecko",
                        symbol="TONUSD",
                        price=float(data['the-open-network']['usd']),
                        bid_price=None,  # CoinGecko doesn't provide bid/ask
                        ask_price=None,
                        volume_24h=None,  # Would need another API call
                        high_24h=None,
                        low_24h=None,
                        base_asset_symbol="TON",
                        quote_asset_symbol="USD"
                    )
                    pairs.append(ton_usd_pair)
                
                # Create a pair for TON/BTC if available
                if 'btc' in data['the-open-network'] and (not quote_asset or quote_asset == "BTC"):
                    ton_btc_pair = SpotPairDTO(
                        exchange_name="CoinGecko",
                        symbol="TONBTC",
                        price=float(data['the-open-network']['btc']),
                        bid_price=None,
                        ask_price=None,
                        volume_24h=None,
                        high_24h=None,
                        low_24h=None,
                        base_asset_symbol="TON",
                        quote_asset_symbol="BTC"
                    )
                    pairs.append(ton_btc_pair)
        except Exception as e:
            print(f"Error fetching TON price from CoinGecko: {e}")
        
        # 2. Try TON Center API if available
        if self.api_token and (not quote_asset or quote_asset == "USD" or quote_asset == "USDT"):
            try:
                endpoint = "/jsonRPC"
                headers = {
                    "X-API-Key": self.api_token
                }
                
                payload = {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "getTokenData",
                    "params": {
                        "address": "TON"
                    }
                }
                
                response = make_request(
                    url=f"{self.base_url}{endpoint}",
                    method="POST",
                    data=payload,
                    headers=headers
                )
                
                data = response.json()
                
                if 'result' in data and 'price' in data['result']:
                    ton_center_price = float(data['result']['price'])
                    
                    # Create a pair for TON/USD
                    ton_usd_pair = SpotPairDTO(
                        exchange_name="TON Center",
                        symbol="TONUSD",
                        price=ton_center_price,
                        bid_price=None,  # TON Center doesn't provide bid/ask
                        ask_price=None,
                        volume_24h=None,
                        high_24h=None,
                        low_24h=None,
                        base_asset_symbol="TON",
                        quote_asset_symbol="USD"
                    )
                    pairs.append(ton_usd_pair)
            except Exception as e:
                print(f"Error fetching TON price from TON Center: {e}")
        
        return pairs
    
    @retry_on_failure(max_retries=3)
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount for TON."""
        if asset != "TON":
            # Only support TON asset
            return 0.0
        
        # For TON, we'll use Fragment data
        try:
            raw_orders = self.scraper.fetch_p2p_from_fragment("TON")
            
            total_amount = 0.0
            for order in raw_orders:
                if order.get('order_type') == order_type:
                    total_amount += float(order.get('available_amount', 0))
            
            return total_amount
        except Exception as e:
            print(f"Error fetching TON available amount: {e}")
            return 0.0