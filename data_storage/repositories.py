# data_storage/repositories.py
from datetime import datetime
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional

from core.models import Exchange, Asset, P2PSnapshot, SpotSnapshot, P2POrder, SpotPair
from core.dto import P2POrderDTO, SpotPairDTO
from config.exchanges import ASSETS, EXCHANGE_SETTINGS

class P2PRepository:
    """Repository for managing P2P market data."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def create_snapshot(self, timestamp: Optional[datetime] = None) -> P2PSnapshot:
        """
        Create a new P2P snapshot.
        
        Args:
            timestamp: Timestamp for the snapshot (optional)
            
        Returns:
            Created P2PSnapshot instance
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        snapshot = P2PSnapshot(timestamp=timestamp)
        self.db.add(snapshot)
        self.db.commit()
        self.db.refresh(snapshot)
        
        return snapshot
    
    def add_order(self, snapshot: P2PSnapshot, order_data: P2POrderDTO) -> P2POrder:
        """
        Add a P2P order to a snapshot.
        
        Args:
            snapshot: Snapshot to add the order to
            order_data: P2P order data transfer object
            
        Returns:
            Created P2POrder instance
        """
        # Get or create Exchange
        exchange = self.db.query(Exchange).filter_by(name=order_data.exchange_name).first()
        if not exchange:
            exchange_settings = next(
                (s for k, s in EXCHANGE_SETTINGS.items() 
                 if k.lower() == order_data.exchange_name.lower() or 
                    s['base_url'].find(order_data.exchange_name.lower()) != -1),
                {}
            )
            exchange = Exchange(
                name=order_data.exchange_name,
                base_url=exchange_settings.get('base_url', ''),
                p2p_url=exchange_settings.get('p2p_url', '')
            )
            self.db.add(exchange)
            self.db.commit()
            self.db.refresh(exchange)
        
        # Get or create Asset
        asset = self.db.query(Asset).filter_by(symbol=order_data.asset_symbol).first()
        if not asset:
            asset = Asset(
                symbol=order_data.asset_symbol,
                name=order_data.asset_symbol
            )
            self.db.add(asset)
            self.db.commit()
            self.db.refresh(asset)
        
        # Create P2POrder
        order = P2POrder(
            exchange_id=exchange.id,
            asset_id=asset.id,
            snapshot_id=snapshot.id,
            price=order_data.price,
            order_type=order_data.order_type,
            available_amount=order_data.available_amount,
            min_amount=order_data.min_amount,
            max_amount=order_data.max_amount,
            payment_methods=order_data.payment_methods,
            order_id=order_data.order_id,
            user_id=order_data.user_id,
            user_name=order_data.user_name,
            completion_rate=order_data.completion_rate
        )
        
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        
        return order
    
    def get_latest_snapshot(self) -> Optional[P2PSnapshot]:
        """
        Get the latest P2P snapshot.
        
        Returns:
            Latest P2PSnapshot instance or None
        """
        return self.db.query(P2PSnapshot).order_by(P2PSnapshot.timestamp.desc()).first()
    
    def get_order_by_external_id(self, order_id: str, exchange_name: str) -> Optional[P2POrder]:
        """
        Get a P2P order by its external ID from a specific exchange.
        
        Args:
            order_id: External order ID
            exchange_name: Exchange name
            
        Returns:
            P2POrder instance or None
        """
        exchange = self.db.query(Exchange).filter_by(name=exchange_name).first()
        if not exchange:
            return None
        
        return self.db.query(P2POrder).join(Exchange).filter(
            P2POrder.order_id == order_id,
            Exchange.id == exchange.id
        ).order_by(P2POrder.created_at.desc()).first()
    
    def find_order_history(self, order_id: str, exchange_name: str, 
                         start_time: datetime, end_time: datetime) -> List[P2POrder]:
        """
        Find history of a specific P2P order.
        
        Args:
            order_id: External order ID
            exchange_name: Exchange name
            start_time: Start time
            end_time: End time
            
        Returns:
            List of P2POrder instances
        """
        exchange = self.db.query(Exchange).filter_by(name=exchange_name).first()
        if not exchange:
            return []
        
        return self.db.query(P2POrder).join(Exchange).join(P2PSnapshot).filter(
            P2POrder.order_id == order_id,
            Exchange.id == exchange.id,
            P2PSnapshot.timestamp >= start_time,
            P2PSnapshot.timestamp <= end_time
        ).order_by(P2PSnapshot.timestamp).all()

class SpotRepository:
    """Repository for managing spot market data."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
    def create_snapshot(self, timestamp: Optional[datetime] = None) -> SpotSnapshot:
        """
        Create a new spot snapshot.
        
        Args:
            timestamp: Timestamp for the snapshot (optional)
            
        Returns:
            Created SpotSnapshot instance
        """
        if timestamp is None:
            timestamp = datetime.now()
        
        snapshot = SpotSnapshot(timestamp=timestamp)
        self.db.add(snapshot)
        self.db.commit()
        self.db.refresh(snapshot)
        
        return snapshot
    
    def add_pair(self, snapshot: SpotSnapshot, pair_data: SpotPairDTO) -> SpotPair:
        """
        Add a spot market pair to a snapshot.
        
        Args:
            snapshot: Snapshot to add the pair to
            pair_data: Spot pair data transfer object
            
        Returns:
            Created SpotPair instance
        """
        # Get or create Exchange
        exchange = self.db.query(Exchange).filter_by(name=pair_data.exchange_name).first()
        if not exchange:
            exchange_settings = next(
                (s for k, s in EXCHANGE_SETTINGS.items() 
                 if k.lower() == pair_data.exchange_name.lower() or 
                    s['base_url'].find(pair_data.exchange_name.lower()) != -1),
                {}
            )
            exchange = Exchange(
                name=pair_data.exchange_name,
                base_url=exchange_settings.get('base_url', ''),
                p2p_url=exchange_settings.get('p2p_url', '')
            )
            self.db.add(exchange)
            self.db.commit()
            self.db.refresh(exchange)
        
        # Get or create Base Asset
        base_asset = self.db.query(Asset).filter_by(symbol=pair_data.base_asset_symbol).first()
        if not base_asset:
            base_asset = Asset(
                symbol=pair_data.base_asset_symbol,
                name=pair_data.base_asset_symbol
            )
            self.db.add(base_asset)
            self.db.commit()
            self.db.refresh(base_asset)
        
        # Get or create Quote Asset
        quote_asset = self.db.query(Asset).filter_by(symbol=pair_data.quote_asset_symbol).first()
        if not quote_asset:
            quote_asset = Asset(
                symbol=pair_data.quote_asset_symbol,
                name=pair_data.quote_asset_symbol
            )
            self.db.add(quote_asset)
            self.db.commit()
            self.db.refresh(quote_asset)
        
        # Create SpotPair
        pair = SpotPair(
            exchange_id=exchange.id,
            base_asset_id=base_asset.id,
            quote_asset_id=quote_asset.id,
            snapshot_id=snapshot.id,
            symbol=pair_data.symbol,
            price=pair_data.price,
            bid_price=pair_data.bid_price,
            ask_price=pair_data.ask_price,
            volume_24h=pair_data.volume_24h,
            high_24h=pair_data.high_24h,
            low_24h=pair_data.low_24h
        )
        
        self.db.add(pair)
        self.db.commit()
        self.db.refresh(pair)
        
        return pair
    
    def get_latest_snapshot(self) -> Optional[SpotSnapshot]:
        """
        Get the latest spot snapshot.
        
        Returns:
            Latest SpotSnapshot instance or None
        """
        return self.db.query(SpotSnapshot).order_by(SpotSnapshot.timestamp.desc()).first()