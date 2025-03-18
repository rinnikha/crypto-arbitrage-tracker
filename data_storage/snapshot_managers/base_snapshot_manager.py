"""
Base snapshot manager with common functionality for all snapshot types.
"""
import logging
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field

from sqlalchemy.orm import Session

from data_collection.api_clients import BaseCollector
from core.utils.concurrency import ConcurrentExecutor
from data_storage.database import SessionLocal

logger = logging.getLogger(__name__)


@dataclass
class CollectionStats:
    """Statistics for data collection operations."""
    item_type: str
    exchange: str
    total_items: int = 0
    successful_items: int = 0
    failed_items: int = 0
    execution_time: float = 0.0
    errors: List[str] = field(default_factory=list)
    assets: Dict[str, int] = field(default_factory=dict)


class BaseSnapshotManager:
    """Base class for snapshot managers."""

    def __init__(self, db_session: Session, collectors: List[BaseCollector]):
        """
        Initialize the snapshot manager.

        Args:
            db_session: SQLAlchemy database session
            collectors: List of data collectors
        """
        self.db_session = db_session
        self.collectors = collectors
        self.executor = ConcurrentExecutor(max_workers=len(collectors) + 1)

    def _create_thread_session(self) -> Session:
        """
        Create a new database session for use in a thread.

        Returns:
            New database session
        """
        try:
            # Use SessionLocal function to get a session
            return SessionLocal()
        except Exception as e:
            logger.error(f"Error creating thread session: {e}")
            # Fallback to creating a new session directly using the engine
            from sqlalchemy.orm import sessionmaker
            from data_storage.database import get_engine

            session_maker = sessionmaker(autocommit=False, autoflush=False, bind=get_engine())
            return session_maker()

    def log_collection_stats(self, stats_list: List[CollectionStats]) -> None:
        """
        Log collection statistics.

        Args:
            stats_list: List of collection statistics
        """
        for stats in stats_list:
            logger.info(
                f"{stats.item_type} collection for {stats.exchange}: "
                f"{stats.successful_items}/{stats.total_items} items in {stats.execution_time:.2f}s"
            )

            if stats.errors:
                errors_str = "\n  - ".join(stats.errors[:5])
                if len(stats.errors) > 5:
                    errors_str += f"\n  ... and {len(stats.errors) - 5} more errors"
                logger.warning(f"Errors from {stats.exchange}:\n  - {errors_str}")

            # Log asset breakdown if available
            if stats.assets:
                assets_str = ", ".join([f"{asset}: {count}" for asset, count in
                                     sorted(stats.assets.items(), key=lambda x: x[1], reverse=True)])
                logger.info(f"  Asset breakdown: {assets_str}")