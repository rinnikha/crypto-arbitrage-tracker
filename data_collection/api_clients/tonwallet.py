# data_collection/api_clients/ton_wallet.py
import requests
import json
from datetime import datetime

from core.models import PricePoint
from data_collection.base import BaseCollector
from core.utils import make_request
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
    
    def fetch_p2p_prices(self, asset):
        """Fetch P2P prices for TON."""
        if asset != "TON":
            # Only support TON asset
            return []
        
        # Use the scraper to get prices from Fragment platform
        return self.scraper.fetch_p2p_prices("TON")
    
    def fetch_exchange_prices(self, asset):
        """Fetch exchange prices for TON from toncenter.com API."""
        if asset != "TON":
            # Only support TON asset
            return None
        
        # For TON, we'll use a combination of sources
        # First try the TON Center API
        if self.api_token:
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
            
            try:
                response = make_request(
                    url=f"{self.base_url}{endpoint}",
                    method="POST",
                    data=payload,
                    headers=headers
                )
                
                data = response.json()
                
                if 'result' in data and 'price' in data['result']:
                    return PricePoint(
                        exchange="TON Center",
                        asset="TON",
                        price=float(data['result']['price']),
                        order_type="MARKET",
                        market_type="EXCHANGE"
                    )
            except Exception as e:
                print(f"Error fetching TON price from TON Center: {e}")
        
        # Fallback to market aggregator
        try:
            response = make_request(
                url="https://api.coingecko.com/api/v3/simple/price",
                method="GET",
                params={
                    "ids": "the-open-network",
                    "vs_currencies": "usd"
                }
            )
            
            data = response.json()
            
            if 'the-open-network' in data and 'usd' in data['the-open-network']:
                return PricePoint(
                    exchange="CoinGecko",
                    asset="TON",
                    price=float(data['the-open-network']['usd']),
                    order_type="MARKET",
                    market_type="EXCHANGE"
                )
        except Exception as e:
            print(f"Error fetching TON price from CoinGecko: {e}")
        
        return None
    
    def fetch_available_amount(self, asset, order_type):
        """Fetch available amount for TON from Fragment platform."""
        if asset != "TON":
            # Only support TON asset
            return 0
        
        return self.scraper.fetch_available_amount("TON", order_type)