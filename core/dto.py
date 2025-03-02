from dataclasses import dataclass
from typing import List, Optional, Any

@dataclass
class PricePointDTO:
    """Data Transfer Object for price points."""
    exchange_name: str
    asset_symbol: str
    price: float
    order_type: str
    market_type: str
    available_amount: Optional[float] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    payment_methods: Optional[List[Any]] = None