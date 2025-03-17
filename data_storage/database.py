# data_storage/database.py
from sqlalchemy import create_engine, QueuePool
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from config.settings import DATABASE_URL

# Create SQLAlchemy engine
engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=10,
    max_overflow=20,
    pool_timeout=30,
    pool_recycle=1800,

    isolation_level="READ COMMITTED",
    client_encoding="utf8",
)

# Create session factory
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base class for models
Base = declarative_base()

def get_db(engine=None):
    """
    Get database session generator.
    
    Args:
        engine: SQLAlchemy engine (optional)
        
    Yields:
        Database session
    """
    if engine is None:
        engine = create_engine(DATABASE_URL)
        
    # Create a SessionLocal instance with the provided engine
    session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    
    db = session_maker()
    try:
        yield db
    finally:
        db.close()