"""
Data Transfer Objects (DTOs) for transferring data between layers.
"""
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from datetime import datetime


@dataclass
class BaseDTO:
    """Base class for all DTOs."""

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert DTO to dictionary.

        Returns:
            Dictionary representation of the DTO
        """
        # Simple implementation - can be enhanced if needed
        return {k: v for k, v in self.__dict__.items() if v is not None}


@dataclass
class P2POrderDTO(BaseDTO):
    """Data Transfer Object for P2P orders."""
    exchange_name: str
    asset_symbol: str
    price: float
    order_type: str  # BUY, SELL

    # Add required field that was missing in the original
    fiat_code: str = "USD"  # Default to USD if not provided

    # Optional fields
    available_amount: Optional[float] = None
    min_amount: Optional[float] = None
    max_amount: Optional[float] = None
    payment_methods: List[Any] = field(default_factory=list)

    # Order tracking fields
    order_id: Optional[str] = None
    user_id: Optional[str] = None
    user_name: Optional[str] = None
    completion_rate: Optional[float] = None

    def __post_init__(self):
        """Validate and normalize data after initialization."""
        # Ensure numeric fields are float
        self.price = float(self.price) if self.price is not None else None

        if self.available_amount is not None:
            self.available_amount = float(self.available_amount)

        if self.min_amount is not None:
            self.min_amount = float(self.min_amount)

        if self.max_amount is not None:
            self.max_amount = float(self.max_amount)

        if self.completion_rate is not None:
            self.completion_rate = float(self.completion_rate)

        # Normalize order type
        if self.order_type:
            self.order_type = self.order_type.upper()

        # Ensure payment methods is a list
        if not self.payment_methods:
            self.payment_methods = []
        elif not isinstance(self.payment_methods, list):
            self.payment_methods = [self.payment_methods]


@dataclass
class SpotPairDTO(BaseDTO):
    """Data Transfer Object for spot market pairs."""
    exchange_name: str
    symbol: str
    price: float

    # Optional fields
    bid_price: Optional[float] = None
    ask_price: Optional[float] = None
    volume_24h: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None

    # Derived fields
    base_asset_symbol: Optional[str] = None
    quote_asset_symbol: Optional[str] = None

    def __post_init__(self):
        """Extract base and quote assets from symbol if not provided and validate data."""
        # Ensure numeric fields are float
        self.price = float(self.price) if self.price is not None else 0.0

        if self.bid_price is not None:
            self.bid_price = float(self.bid_price)

        if self.ask_price is not None:
            self.ask_price = float(self.ask_price)

        if self.volume_24h is not None:
            self.volume_24h = float(self.volume_24h)

        if self.high_24h is not None:
            self.high_24h = float(self.high_24h)

        if self.low_24h is not None:
            self.low_24h = float(self.low_24h)

        # Extract base and quote assets from symbol if not provided
        if not self.base_asset_symbol or not self.quote_asset_symbol:
            self._extract_base_quote_from_symbol()

    def _extract_base_quote_from_symbol(self) -> None:
        """Extract base and quote assets from trading pair symbol."""
        # Common quote assets ordered by length (longest first) to avoid false matches
        common_quote_assets = sorted(
            ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB", "EUR", "GBP", "JPY"],
            key=len,
            reverse=True
        )

        if not self.symbol:
            return

        # Try to extract quote asset
        for quote in common_quote_assets:
            if self.symbol.endswith(quote):
                self.quote_asset_symbol = quote
                self.base_asset_symbol = self.symbol[:-len(quote)]
                return

        # If not matched, use a fallback approach (most markets use 3-4 chars for quote asset)
        symbol_len = len(self.symbol)
        if symbol_len > 3:
            if symbol_len >= 8:  # Longer symbols like BTCUSDT
                self.base_asset_symbol = self.symbol[:-4]
                self.quote_asset_symbol = self.symbol[-4:]
            else:  # Shorter symbols
                self.base_asset_symbol = self.symbol[:-3]
                self.quote_asset_symbol = self.symbol[-3:]


@dataclass
class ArbitrageOpportunityDTO(BaseDTO):
    """Data Transfer Object for arbitrage opportunities."""
    buy_exchange: str
    sell_exchange: str
    asset: str
    buy_price: float
    sell_price: float
    available_amount: float
    transfer_method: str
    transfer_fee: float
    potential_profit: float
    profit_percentage: float
    timestamp: datetime

    # Optional fields
    execution_time_minutes: Optional[int] = 120  # Default to 2 hours

    def __post_init__(self):
        """Validate and derive additional data."""
        # Ensure numeric fields are float
        self.buy_price = float(self.buy_price)
        self.sell_price = float(self.sell_price)
        self.available_amount = float(self.available_amount)
        self.transfer_fee = float(self.transfer_fee)
        self.potential_profit = float(self.potential_profit)
        self.profit_percentage = float(self.profit_percentage)

        if self.execution_time_minutes is not None:
            self.execution_time_minutes = int(self.execution_time_minutes)

    def get_hourly_roi(self) -> float:
        """
        Calculate hourly ROI.

        Returns:
            Hourly ROI percentage
        """
        if not self.execution_time_minutes or self.execution_time_minutes <= 0:
            return 0.0

        hours = self.execution_time_minutes / 60
        return self.profit_percentage / hours if hours > 0 else 0.0

    def get_daily_roi(self) -> float:
        """
        Calculate daily ROI (assuming continuous trading).

        Returns:
            Daily ROI percentage
        """
        hourly_roi = self.get_hourly_roi()
        return hourly_roi * 24


@dataclass
class SnapshotResultDTO(BaseDTO):
    """Data Transfer Object for snapshot results."""
    snapshot_id: int
    timestamp: datetime
    total_items: int
    execution_time: float

    # Exchange-specific stats
    items_by_exchange: Dict[str, int] = field(default_factory=dict)

    # Additional stats
    errors: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Validate data."""
        # Ensure numeric fields have proper types
        self.snapshot_id = int(self.snapshot_id)
        self.total_items = int(self.total_items)
        self.execution_time = float(self.execution_time)