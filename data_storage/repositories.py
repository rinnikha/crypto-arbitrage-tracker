# data_storage/repositories.py
from datetime import datetime
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional

from core.dto import PricePointDTO
from core.models import Exchange, Asset, Snapshot, PricePoint, TransferFee
from config.exchanges import ASSETS, EXCHANGE_SETTINGS, TRANSFER_METHODS

class SnapshotRepository:
    """Repository for managing Snapshot data."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def create_snapshot(self, timestamp: Optional[datetime] = None) -> Snapshot:
        """
        Create a new snapshot.
        
        Args:
            timestamp: Timestamp for the snapshot (optional)
            
        Returns:
            Created Snapshot instance
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        snapshot = Snapshot(timestamp=timestamp)
        self.db.add(snapshot)
        self.db.commit()
        self.db.refresh(snapshot)
        
        return snapshot
    
    def add_price_point(self, snapshot: Snapshot, price_data: PricePointDTO) -> PricePoint:
        """
        Add a price point to a snapshot.
        
        Args:
            snapshot: Snapshot to add the price point to
            price_data: Price point data transfer object
            
        Returns:
            Created PricePoint instance
        """
        # Get or create Exchange
        exchange = self.db.query(Exchange).filter_by(name=price_data.exchange_name).first()
        if not exchange:
            exchange_settings = next(
                (s for k, s in EXCHANGE_SETTINGS.items() if s['base_url'].find(price_data.exchange_name.lower()) != -1),
                {}
            )
            exchange = Exchange(
                name=price_data.exchange_name,
                base_url=exchange_settings.get('base_url', ''),
                p2p_url=exchange_settings.get('p2p_url', '')
            )
            self.db.add(exchange)
            self.db.commit()
            self.db.refresh(exchange)
        
        # Get or create Asset
        asset = self.db.query(Asset).filter_by(symbol=price_data.asset_symbol).first()
        if not asset:
            asset = Asset(
                symbol=price_data.asset_symbol,
                name=price_data.asset_symbol
            )
            self.db.add(asset)
            self.db.commit()
            self.db.refresh(asset)
        
        # Create PricePoint
        price_point = PricePoint(
            exchange_id=exchange.id,
            asset_id=asset.id,
            snapshot_id=snapshot.id,
            price=price_data.price,
            order_type=price_data.order_type,
            market_type=price_data.market_type,
            available_amount=price_data.available_amount,
            min_amount=price_data.min_amount,
            max_amount=price_data.max_amount,
            payment_methods=price_data.payment_methods
        )
        
        self.db.add(price_point)
        self.db.commit()
        self.db.refresh(price_point)
        
        return price_point
    
    def get_latest_snapshot(self) -> Optional[Snapshot]:
        """
        Get the latest snapshot.
        
        Returns:
            Latest Snapshot instance or None
        """
        return self.db.query(Snapshot).order_by(Snapshot.timestamp.desc()).first()
    
    def get_snapshots_in_range(self, start_time: datetime, end_time: datetime) -> List[Snapshot]:
        """
        Get snapshots in a time range.
        
        Args:
            start_time: Start of the time range
            end_time: End of the time range
            
        Returns:
            List of Snapshot instances
        """
        return self.db.query(Snapshot).filter(
            Snapshot.timestamp >= start_time,
            Snapshot.timestamp <= end_time
        ).order_by(Snapshot.timestamp).all()

class PricePointRepository:
    """Repository for managing PricePoint data."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def get_latest_prices(self, asset_symbol: str, market_type: Optional[str] = None) -> List[PricePoint]:
        """
        Get the latest prices for an asset.
        
        Args:
            asset_symbol: Symbol of the asset
            market_type: Type of market (P2P or EXCHANGE)
            
        Returns:
            List of PricePoint instances
        """
        # Get the latest snapshot
        latest_snapshot = self.db.query(Snapshot).order_by(Snapshot.timestamp.desc()).first()
        if not latest_snapshot:
            return []
        
        # Get asset ID
        asset = self.db.query(Asset).filter_by(symbol=asset_symbol).first()
        if not asset:
            return []
        
        # Build query
        query = self.db.query(PricePoint).filter(
            PricePoint.snapshot_id == latest_snapshot.id,
            PricePoint.asset_id == asset.id
        )
        
        if market_type:
            query = query.filter(PricePoint.market_type == market_type)
        
        return query.all()
    
    def get_price_history(self, asset_symbol: str, exchange_name: str, 
                         order_type: str, market_type: str, 
                         start_time: datetime, end_time: datetime) -> List[PricePoint]:
        """
        Get price history for an asset on a specific exchange.
        
        Args:
            asset_symbol: Symbol of the asset
            exchange_name: Name of the exchange
            order_type: Type of order (BUY, SELL, MARKET)
            market_type: Type of market (P2P, EXCHANGE)
            start_time: Start of the time range
            end_time: End of the time range
            
        Returns:
            List of PricePoint instances
        """
        # Get asset and exchange IDs
        asset = self.db.query(Asset).filter_by(symbol=asset_symbol).first()
        exchange = self.db.query(Exchange).filter_by(name=exchange_name).first()
        
        if not asset or not exchange:
            return []
        
        # Get snapshots in range
        snapshots = self.db.query(Snapshot).filter(
            Snapshot.timestamp >= start_time,
            Snapshot.timestamp <= end_time
        ).all()
        
        if not snapshots:
            return []
        
        # Get price points
        snapshot_ids = [s.id for s in snapshots]
        
        price_points = self.db.query(PricePoint).filter(
            PricePoint.asset_id == asset.id,
            PricePoint.exchange_id == exchange.id,
            PricePoint.order_type == order_type,
            PricePoint.market_type == market_type,
            PricePoint.snapshot_id.in_(snapshot_ids)
        ).join(Snapshot).order_by(Snapshot.timestamp).all()
        
        return price_points