"""
Database connection and session management with optimized connection pooling.
"""
import logging
import threading
from typing import Generator, Optional, Dict, Any
from contextlib import contextmanager

from sqlalchemy import create_engine, event
from sqlalchemy.exc import SQLAlchemyError, DisconnectionError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session, scoped_session
from sqlalchemy.pool import QueuePool

from core.errors import DatabaseError
from config.manager import get_config

# Import for type hints only
import sqlalchemy.orm

logger = logging.getLogger(__name__)

# Global variables
_engine = None
_session_factory = None
_session_registry = threading.local()
_engine_lock = threading.RLock()

# Create base class for models
Base = declarative_base()


# Session factory for direct use (needed by snapshot managers)
# Create a placeholder function that will initialize the engine if needed
def SessionLocal():
    """
    Get a session from the session factory, initializing the engine if needed.

    Returns:
        SQLAlchemy session
    """
    global _session_factory

    if _session_factory is None:
        # Initialize engine if not already done
        init_engine()

    if _session_factory is None:
        raise DatabaseError("Failed to initialize session factory")

    return _session_factory()


def _check_connection(dbapi_connection, connection_record, connection_proxy):
    """
    Validate connection before use.

    This callback ensures connections are valid before being used,
    preventing "stale connection" errors.
    """
    cursor = dbapi_connection.cursor()
    try:
        cursor.execute("SELECT 1")
    except Exception:
        # If connection is invalid, create a new one
        logger.warning("Invalid connection detected, reconnecting...")
        raise DisconnectionError()
    finally:
        cursor.close()


def init_engine(db_url: Optional[str] = None, **engine_kwargs) -> None:
    """
    Initialize the database engine with connection pooling.

    Args:
        db_url: Database URL (defaults to config)
        **engine_kwargs: Additional engine parameters
    """
    global _engine, _session_factory

    # Use lock to prevent race conditions during initialization
    with _engine_lock:
        if _engine is not None:
            # Engine already initialized
            return

        # Get database URL from config if not provided
        config = get_config()
        db_url = db_url or config.require('DATABASE_URL')

        # Default engine parameters
        default_params = {
            'pool_size': config.get_int('DB_POOL_SIZE', 10),
            'max_overflow': config.get_int('DB_MAX_OVERFLOW', 20),
            'pool_timeout': config.get_int('DB_POOL_TIMEOUT', 30),
            'pool_recycle': config.get_int('DB_POOL_RECYCLE', 1800),
            'pool_pre_ping': True,  # Verify connections before using them
            'isolation_level': config.get('DB_ISOLATION_LEVEL', 'READ COMMITTED'),
            'connect_args': {
                'connect_timeout': config.get_int('DB_CONNECT_TIMEOUT', 10),
                'keepalives': 1,
                'keepalives_idle': 300,
                'keepalives_interval': 10,
                'keepalives_count': 5,
            }
        }

        # Override defaults with provided parameters
        params = {**default_params, **engine_kwargs}

        try:
            # Create the engine
            logger.info(f"Initializing database engine with URL: {db_url}")
            engine = create_engine(
                db_url,
                poolclass=QueuePool,
                **params
            )

            # Add connection validation event listener
            event.listen(engine, 'checkout', _check_connection)

            # Create session factory
            session_factory = sessionmaker(
                autocommit=False,
                autoflush=False,
                bind=engine
            )

            # Set global variables
            _engine = engine
            _session_factory = session_factory

            logger.info("Database engine initialized successfully")

        except Exception as e:
            logger.exception(f"Error initializing database engine: {e}")
            raise DatabaseError(f"Failed to initialize database engine: {e}")


def get_engine():
    """
    Get the database engine.

    Returns:
        SQLAlchemy engine instance

    Raises:
        DatabaseError: If engine not initialized
    """
    if _engine is None:
        init_engine()

    return _engine


def get_session_factory():
    """
    Get the session factory.

    Returns:
        SQLAlchemy session factory

    Raises:
        DatabaseError: If engine not initialized
    """
    if _session_factory is None:
        init_engine()

    return _session_factory


def get_db_session() -> Session:
    """
    Get a new database session.

    Returns:
        SQLAlchemy session

    Raises:
        DatabaseError: If session cannot be created
    """
    try:
        if not hasattr(_session_registry, 'session'):
            # Create a new session
            _session_registry.session = get_session_factory()()

        return _session_registry.session
    except Exception as e:
        logger.exception(f"Error creating database session: {e}")
        raise DatabaseError(f"Failed to create database session: {e}")


@contextmanager
def session_scope() -> Generator[Session, None, None]:
    """
    Provide a transactional scope around a series of operations.

    This context manager handles session creation, commit, and cleanup.

    Yields:
        SQLAlchemy session

    Raises:
        DatabaseError: If session operations fail
    """
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception as e:
        session.rollback()
        logger.exception(f"Error in database session: {e}")
        raise DatabaseError(f"Database error: {e}")
    finally:
        session.close()


def get_db(engine=None) -> Generator[Session, None, None]:
    """
    Get database session generator.

    Legacy function for backward compatibility.
    For new code, prefer using session_scope.

    Args:
        engine: SQLAlchemy engine (optional)

    Yields:
        Database session
    """
    if engine is None:
        engine = get_engine()

    # Create a session
    session_maker = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    session = session_maker()

    try:
        yield session
    finally:
        session.close()


def create_tables() -> None:
    """
    Create all tables defined in models.

    Use with caution in production.
    """
    engine = get_engine()
    Base.metadata.create_all(bind=engine)
    logger.info("Created all database tables")


def drop_tables() -> None:
    """
    Drop all tables defined in models.

    Use with extreme caution.
    """
    engine = get_engine()
    Base.metadata.drop_all(bind=engine)
    logger.info("Dropped all database tables")


def execute_raw_query(query: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """
    Execute a raw SQL query.

    Args:
        query: SQL query string
        params: Query parameters

    Returns:
        Query result

    Raises:
        DatabaseError: If query execution fails
    """
    engine = get_engine()

    try:
        with engine.connect() as connection:
            result = connection.execute(query, params or {})
            return result
    except SQLAlchemyError as e:
        logger.exception(f"Error executing raw query: {e}")
        raise DatabaseError(f"Error executing database query: {e}")


def check_connection() -> bool:
    """
    Check if database connection is working.

    Returns:
        True if connection is working, False otherwise
    """
    try:
        engine = get_engine()
        with engine.connect() as connection:
            connection.execute("SELECT 1")
        return True
    except Exception as e:
        logger.error(f"Database connection check failed: {e}")
        return False


def close_sessions() -> None:
    """Close all open sessions in the current thread."""
    if hasattr(_session_registry, 'session'):
        try:
            _session_registry.session.close()
        except Exception as e:
            logger.warning(f"Error closing session: {e}")

        delattr(_session_registry, 'session')


def dispose_engine() -> None:
    """
    Dispose the database engine.

    This closes all connections and should be called on application shutdown.
    """
    global _engine

    if _engine is not None:
        try:
            _engine.dispose()
            logger.info("Database engine disposed")
        except Exception as e:
            logger.error(f"Error disposing database engine: {e}")

        _engine = None