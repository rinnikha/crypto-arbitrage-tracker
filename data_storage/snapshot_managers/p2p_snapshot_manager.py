import logging
import time
from datetime import datetime
from typing import List, Dict, Any

from sqlalchemy.orm import Session

from data_collection.api_clients import BaseCollector
from data_storage.repositories import P2PRepository
from data_storage.snapshot_managers import BaseSnapshotManager, CollectionStats

logger = logging.getLogger(__name__)

class P2PSnapshotManager(BaseSnapshotManager):
    """Manager for creating snapshots of P2P market data."""

    def __init__(self, db_session: Session, collectors: List[BaseCollector], assets: List[str]):
        """
        Initialize the P2P snapshot manager.

        Args:
            db_session: SQLAlchemy database session
            collectors: List of data collectors
            assets: List of asset symbols to track
        """
        super().__init__(db_session, collectors)
        self.repository = P2PRepository(db_session)
        self.assets = assets

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

        logger.info(f"Created P2P snapshot {snapshot_id} at {timestamp}")

        # Collect data from all exchanges concurrently
        collection_tasks = []

        for collector in self.collectors:
            exchange_name = collector.get_exchange_name()
            fiats = ['USD', 'RUB', 'EUR', 'UZS']
            for asset in self.assets:
                collection_tasks.append((collector, asset, fiats, exchange_name))

        # Execute collection tasks concurrently
        results = self.executor.execute(
            items=collection_tasks,
            worker_func=lambda task: self._collect_p2p_orders(task[0], task[1], task[2], task[3], snapshot_id)
        )

        # Process results
        total_orders = 0
        total_failures = 0
        exchange_stats: Dict[str, CollectionStats] = {}

        for result in results:
            if not result.success or not result.result:
                total_failures += 1
                continue

            stats = result.result
            if stats.exchange not in exchange_stats:
                exchange_stats[stats.exchange] = stats
            else:
                # Merge stats
                existing = exchange_stats[stats.exchange]
                existing.total_items += stats.total_items
                existing.successful_items += stats.successful_items
                existing.failed_items += stats.failed_items
                existing.errors.extend(stats.errors)

                # Merge asset counts
                for asset, count in stats.assets.items():
                    existing.assets[asset] = existing.assets.get(asset, 0) + count

            total_orders += stats.successful_items

        # Log collection statistics
        self.log_collection_stats(list(exchange_stats.values()))

        # Calculate execution time
        execution_time = time.time() - start_time

        # Prepare result
        result = {
            "snapshot_id": snapshot_id,
            "timestamp": timestamp,
            "total_orders": total_orders,
            "execution_time": execution_time,
            "orders_by_exchange": {
                stats.exchange: stats.successful_items
                for stats in exchange_stats.values()
            }
        }

        logger.info(
            f"P2P snapshot {snapshot_id} completed in {execution_time:.2f}s "
            f"with {total_orders} orders from {len(exchange_stats)} exchanges"
        )

        return result

    def _collect_p2p_orders(self, collector: BaseCollector, asset: str, fiats: List[str],
                            exchange_name: str, snapshot_id: int) -> CollectionStats:
        """
        Collect P2P orders for a specific asset from a collector.
        This method runs in a separate thread.

        Args:
            collector: Data collector
            asset: Asset symbol
            exchange_name: Exchange name
            snapshot_id: Snapshot ID

        Returns:
            Collection statistics
        """
        start_time = time.time()

        # Create stats object
        stats = CollectionStats(
            item_type="P2P Orders",
            exchange=exchange_name,
            assets={asset: 0}
        )

        # Create thread-local session and repository
        thread_session = self._create_thread_session()
        thread_repo = P2PRepository(thread_session)

        try:
            # Get the snapshot
            snapshot = thread_repo.get_snapshot_by_id(snapshot_id)
            if not snapshot:
                stats.errors.append(f"Snapshot {snapshot_id} not found")
                return stats

            # Collect orders
            logger.info(f"Collecting P2P orders for {asset} from {exchange_name}")

            try:
                orders = collector.fetch_p2p_orders(asset, fiats)
                stats.total_items = len(orders)
                stats.assets[asset] = len(orders)

                if orders:
                    # Add orders to snapshot
                    thread_repo.add_orders_batch(snapshot, orders)
                    stats.successful_items = len(orders)

                    logger.info(
                        f"Added {len(orders)} P2P orders for {asset} from {exchange_name} "
                        f"to snapshot {snapshot_id}"
                    )
                else:
                    logger.warning(f"No P2P orders found for {asset} from {exchange_name}")
            except Exception as e:
                error_msg = f"Error collecting P2P orders for {asset} from {exchange_name}: {str(e)}"
                logger.exception(error_msg)
                stats.errors.append(error_msg)
                stats.failed_items = 1
        except Exception as e:
            error_msg = f"Error in P2P collection thread for {asset} from {exchange_name}: {str(e)}"
            logger.exception(error_msg)
            stats.errors.append(error_msg)
        finally:
            # Close thread-local session
            thread_session.close()

            # Update execution time
            stats.execution_time = time.time() - start_time

        return stats
