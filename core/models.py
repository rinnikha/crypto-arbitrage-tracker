from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, JSON
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
    created_at = Column(DateTime, default=datetime.now)
    
    price_points = relationship("PricePoint", back_populates="exchange")

class Asset(Base):
    """Model for cryptocurrency assets."""
    __tablename__ = "assets"
    
    id = Column(Integer, primary_key=True)
    symbol = Column(String, unique=True, nullable=False)
    name = Column(String)
    created_at = Column(DateTime, default=datetime.now)
    
    price_points = relationship("PricePoint", back_populates="asset")

class Snapshot(Base):
    """Model for snapshots of exchange data."""
    __tablename__ = "snapshots"
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.now, nullable=False)
    
    price_points = relationship("PricePoint", back_populates="snapshot")

class PricePoint(Base):
    """Model for price points within a snapshot."""
    __tablename__ = "price_points"
    
    id = Column(Integer, primary_key=True)
    exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    snapshot_id = Column(Integer, ForeignKey("snapshots.id"), nullable=False)
    price = Column(Float, nullable=False)
    order_type = Column(String, nullable=False)  # BUY, SELL, MARKET
    market_type = Column(String, nullable=False)  # P2P, EXCHANGE
    available_amount = Column(Float)
    min_amount = Column(Float)
    max_amount = Column(Float)
    payment_methods = Column(JSON)
    created_at = Column(DateTime, default=datetime.now)
    
    exchange = relationship("Exchange", back_populates="price_points")
    asset = relationship("Asset", back_populates="price_points")
    snapshot = relationship("Snapshot", back_populates="price_points")

class TransferFee(Base):
    """Model for transfer fees between exchanges."""
    __tablename__ = "transfer_fees"
    
    id = Column(Integer, primary_key=True)
    from_exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    to_exchange_id = Column(Integer, ForeignKey("exchanges.id"), nullable=False)
    asset_id = Column(Integer, ForeignKey("assets.id"), nullable=False)
    fee_fixed = Column(Float, default=0)
    fee_percentage = Column(Float, default=0)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)