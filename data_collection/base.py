from abc import ABC, abstractmethod
from core.dto import PricePointDTO

class BaseCollector(ABC):
    """Base class for all data collectors."""

    @abstractmethod
    def fetch_p2p_prices(self, assets):
        """Fetch P2P market prices for the given asset"""

    @abstractmethod
    def fetch_exchange_prices(self, asset):
        """Fetch direct exchange prices for the given asset."""
        pass

    @abstractmethod
    def fetch_available_amount(self, asset, order_type):
        """Fetch available amount for the give asset and order type."""
        pass