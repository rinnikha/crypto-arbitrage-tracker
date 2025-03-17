# core/dto.py
from dataclasses import dataclass
from typing import List, Optional, Any, Dict
from datetime import datetime

@dataclass
class P2POrderDTO:
    """Data Transfer Object for P2P orders."""
    exchange_name: str
    asset_symbol: str
    fiat_code: str
    price: float
    order_type: str  # BUY, SELL
    available_amount: Optional[float] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    payment_methods: Optional[List[Any]] = None
    
    # New fields for order tracking
    order_id: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    completion_rate: Optional[float] = None

@dataclass
class SpotPairDTO:
    """Data Transfer Object for spot market pairs."""
    exchange_name: str
    symbol: str
    price: float
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    volume_24h: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None
    
    # Derived fields
    base_asset_symbol: Optional[str] = None
    quote_asset_symbol: Optional[str] = None
    
    def __post_init__(self):
        """Extract base and quote assets from symbol if not provided."""
        if not self.base_asset_symbol or not self.quote_asset_symbol:
            # Common pattern is BTCUSDT where BTC is base and USDT is quote
            # This is a simple implementation - might need adjustment for some exchanges
            common_quote_assets = ["USDT", "USDC", "USD", "BTC", "ETH", "BNB"]
            
            for quote in common_quote_assets:
                if self.symbol.endswith(quote):
                    self.quote_asset_symbol = quote
                    self.base_asset_symbol = self.symbol[:-len(quote)]
                    break