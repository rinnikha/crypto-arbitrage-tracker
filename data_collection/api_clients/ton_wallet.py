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
        pass
    
    @retry_on_failure(max_retries=3)
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount for TON."""
        pass