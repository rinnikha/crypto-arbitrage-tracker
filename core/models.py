# core/models.py
from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON, Boolean, Index, ARRAY
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime

Base = declarative_base()

class Exchange(Base):
    """Model for cryptocurrency exchanges."""
    __tablename__ = "exchanges"
    
    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    base_url = Column(String)
    p2p_url = Column(String)
    fiat_currencies = Column(ARRAY(String), default=[])

    created_at = Column(DateTime, default=datetime.now)
    
    p2p_orders = relationship("P2POrder", back_populates="exchange")
    spot_pairs = relationship("SpotPair", back_populates="exchange")
    payment_method_mappers = relationship("PaymentMethodMapper", back_populates="exchange")

class Asset(Base):
    """Model for cryptocurrency assets."""
    __tablename__ = "assets"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    
    base_pairs = relationship("SpotPair", foreign_keys="SpotPair.base_asset_id", back_populates="base_asset")
    quote_pairs = relationship("SpotPair", foreign_keys="SpotPair.quote_asset_id", back_populates="quote_asset")
    p2p_orders = relationship("P2POrder", back_populates="asset")

class Fiat(Base):
    """Model for cryptocurrency assets."""
    __tablename__ = "fiats"
    
    id = Column(Integer, primary_key=True)
    code = Column(String, unique=True, nullable=False)
    name = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    
    p2p_orders = relationship("P2POrder", back_populates="fiat")
    payment_methods = relationship("PaymentMethod", back_populates="fiat")

class PaymentMethod(Base):
    __tablename__ = "payment_methods"

    id = Column(Integer, primary_key=True)
    name = Column(String, unique=True, nullable=False)
    created_at = Column(DateTime, default=datetime.now)
    fiat_id = Column(Integer, ForeignKey("fiats.id"))

    payment_method_mappers = relationship("PaymentMethodMapper", back_populates="payment_method")

    fiat = relationship("Fiat", back_populates="payment_methods")

class PaymentMethodMapper(Base):
    __tablename__ = "payment_method_mappers"

    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"))
    payment_method_id = Column(Integer, ForeignKey("payment_methods.id"))
    mapping_key = Column(String, unique=False, nullable=False)
    created_at = Column(DateTime, default=datetime.now)

    exchange = relationship("Exchange", back_populates="payment_method_mappers")
    payment_method = relationship("PaymentMethod", back_populates="payment_method_mappers")


class P2PSnapshot(Base):
    """Model for snapshots of P2P market data."""
    __tablename__ = "p2p_snapshots"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now, nullable=False)
    
    orders = relationship("P2POrder", back_populates="snapshot")

class SpotSnapshot(Base):
    """Model for snapshots of spot market data."""
    __tablename__ = "spot_snapshots"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now, nullable=False)
    
    pairs = relationship("SpotPair", back_populates="snapshot")

class P2POrder(Base):
    """Model for P2P orders within a snapshot."""
    __tablename__ = "p2p_orders"
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    fiat_id = Column(Integer, ForeignKey("fiats.id"), nullable=False)
    snapshot_id = Column(Integer, ForeignKey("p2p_snapshots.id"), nullable=False)
    
    # Order details
    price = Column(Float, nullable=False)
    order_type = Column(String, nullable=False)  # BUY, SELL
    available_amount = Column(Float)
    min_amount = Column(Float)
    max_amount = Column(Float)
    payment_methods = Column(JSONB)
    
    # New fields for order tracking
    order_id = Column(String, nullable=False)  # External order ID from exchange
    user_id = Column(String, nullable=True)   # User ID from exchange
    user_name = Column(String, nullable=True) # Username from exchange
    completion_rate = Column(Float)  # User's completion rate if available
    
    # Tracking data
    created_at = Column(DateTime, default=datetime.now)
    
    exchange = relationship("Exchange", back_populates="p2p_orders")
    asset = relationship("Asset", back_populates="p2p_orders")
    fiat = relationship("Fiat", back_populates="p2p_orders")
    snapshot = relationship("P2PSnapshot", back_populates="orders")

    __table_args__ = (
        Index('idx_p2p_orders_snapshot_exchange_asset', 'snapshot_id', 'exchange_id', 'asset_id'),
        Index('idx_p2p_orders_order_id', 'order_id')
    )

class SpotPair(Base):
    """Model for spot market trading pairs."""
    __tablename__ = "spot_pairs"
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    base_asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    quote_asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    snapshot_id = Column(Integer, ForeignKey("spot_snapshots.id"), nullable=False)
    
    # Market data
    symbol = Column(String, nullable=False)  # Trading pair symbol (e.g., BTCUSDT)
    price = Column(Float, nullable=False)
    bid_price = Column(Float)
    ask_price = Column(Float)
    volume_24h = Column(Float)
    high_24h = Column(Float)
    low_24h = Column(Float)
    
    # Tracking data
    created_at = Column(DateTime, default=datetime.now)
    
    exchange = relationship("Exchange", back_populates="spot_pairs")
    base_asset = relationship("Asset", foreign_keys=[base_asset_id], back_populates="base_pairs")
    quote_asset = relationship("Asset", foreign_keys=[quote_asset_id], back_populates="quote_pairs")
    snapshot = relationship("SpotSnapshot", back_populates="pairs")
