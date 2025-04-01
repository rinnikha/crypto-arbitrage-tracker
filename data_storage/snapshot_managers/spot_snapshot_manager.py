import logging
import time
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.orm import Session

from data_collection.api_clients import BaseCollector
from data_storage.repositories import SpotRepository
from data_storage.snapshot_managers import BaseSnapshotManager, CollectionStats

logger = logging.getLogger(__name__)

class SpotSnapshotManager(BaseSnapshotManager):
    """Manager for creating snapshots of spot market data."""

    def __init__(self, db_session: Session, collectors: List[BaseCollector],
                 base_assets: List[str], quote_assets: List[str]):
        """
        Initialize the spot snapshot manager.

        Args:
            db_session: SQLAlchemy database session
            collectors: List of data collectors
            base_assets: List of base asset symbols to track
            quote_assets: List of quote asset symbols to track
        """
        super().__init__(db_session, collectors)
        self.repository = SpotRepository(db_session)
        self.base_assets = base_assets
        self.quote_assets = quote_assets

    def create_snapshot(self) -> Dict[str, Any]:
        """
        Create a new snapshot synchronously.
        This is provided for backward compatibility.

        Returns:
            Dictionary with snapshot info
        """
        return self.create_snapshot_concurrent()

    def create_snapshot_concurrent(self) -> Dict[str, Any]:
        """
        Create a new snapshot with concurrent data collection.

        Returns:
            Dictionary with snapshot info
        """
        start_time = time.time()
        timestamp = datetime.now()

        # Create the snapshot
        snapshot = self.repository.create_snapshot(timestamp)
        snapshot_id = snapshot.id

        logger.info(f"Created spot snapshot {snapshot_id} at {timestamp}")

        # Collect data from all exchanges concurrently
        collection_tasks = [(collector, collector.get_exchange_name()) for collector in self.collectors]

        # Execute collection tasks concurrently
        results = self.executor.execute(
            items=collection_tasks,
            worker_func=lambda task: self._collect_spot_pairs(task[0], task[1], snapshot_id)
        )

        # Process results
        total_pairs = 0
        total_failures = 0
        exchange_stats: Dict[str, CollectionStats] = {}

        for result in results:
            if not result.success or not result.result:
                total_failures += 1
                continue

            stats = result.result
            exchange_stats[stats.exchange] = stats
            total_pairs += stats.successful_items

        # Log collection statistics
        self.log_collection_stats(list(exchange_stats.values()))

        # Calculate execution time
        execution_time = time.time() - start_time

        # Prepare result
        result = {
            "snapshot_id": snapshot_id,
            "timestamp": timestamp,
            "total_pairs": total_pairs,
            "execution_time": execution_time,
            "pairs_by_exchange": {
                stats.exchange: stats.successful_items
                for stats in exchange_stats.values()
            }
        }

        logger.info(
            f"Spot snapshot {snapshot_id} completed in {execution_time:.2f}s "
            f"with {total_pairs} pairs from {len(exchange_stats)} exchanges"
        )

        return result

    def _collect_spot_pairs(self, collector: BaseCollector,
                            exchange_name: str, snapshot_id: int) -> CollectionStats:
        """
        Collect spot pairs from a collector.
        This method runs in a separate thread.

        Args:
            collector: Data collector
            exchange_name: Exchange name
            snapshot_id: Snapshot ID

        Returns:
            Collection statistics
        """
        start_time = time.time()

        # Create stats object
        stats = CollectionStats(
            item_type="Spot Pairs",
            exchange=exchange_name
        )

        # Create thread-local session and repository
        thread_session = self._create_thread_session()
        thread_repo = SpotRepository(thread_session)

        try:
            # Get the snapshot
            snapshot = thread_repo.get_snapshot_by_id(snapshot_id)
            if not snapshot:
                stats.errors.append(f"Snapshot {snapshot_id} not found")
                return stats

            # Collect spot pairs
            logger.info(f"Collecting spot pairs from {exchange_name}")

            try:
                # Method 1: Try to get all pairs at once
                all_pairs = collector.fetch_spot_pairs()

                # Filter pairs by configured assets
                filtered_pairs = []
                for pair in all_pairs:
                    filtered_pairs.append(pair)

                    # Update asset stats
                    base_symbol = pair.base_asset_symbol
                    stats.assets[base_symbol] = stats.assets.get(base_symbol, 0) + 1

                stats.total_items = len(filtered_pairs)

                if filtered_pairs:
                    # Add pairs to snapshot
                    thread_repo.add_pairs_batch(snapshot, filtered_pairs)
                    stats.successful_items = len(filtered_pairs)

                    logger.info(
                        f"Added {len(filtered_pairs)} spot pairs from {exchange_name} "
                        f"to snapshot {snapshot_id}"
                    )
                else:
                    logger.warning(f"No relevant spot pairs found from {exchange_name}")
            except Exception as e:
                error_msg = f"Error collecting spot pairs from {exchange_name}: {str(e)}"
                logger.exception(error_msg)
                stats.errors.append(error_msg)

        except Exception as e:
            error_msg = f"Error in spot collection thread for {exchange_name}: {str(e)}"
            logger.exception(error_msg)
            stats.errors.append(error_msg)
        finally:
            # Close thread-local session
            thread_session.close()

            # Update execution time
            stats.execution_time = time.time() - start_time

        return stats
