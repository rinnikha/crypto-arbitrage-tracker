# data_collection/base.py
from abc import ABC, abstractmethod
from typing import List, Optional
from core.dto import P2POrderDTO, SpotPairDTO

class BaseCollector(ABC):
    """Base class for all data collectors."""
    
    @abstractmethod
    def fetch_p2p_orders(self, asset: str) -> List[P2POrderDTO]:
        """Fetch P2P market orders for the given asset."""
        pass
    
    @abstractmethod
    def fetch_spot_pairs(self, base_asset: Optional[str] = None, 
                        quote_asset: Optional[str] = None) -> List[SpotPairDTO]:
        """
        Fetch spot market pairs.
        
        Args:
            base_asset: Filter by base asset (optional)
            quote_asset: Filter by quote asset (optional)
            
        Returns:
            List of spot market pairs
        """
        pass
    
    @abstractmethod
    def fetch_available_amount(self, asset: str, order_type: str) -> float:
        """Fetch available amount for the given asset and order type."""
        pass