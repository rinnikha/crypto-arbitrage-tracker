# data_storage/snapshots.py
import time
from datetime import datetime
import concurrent.futures
from typing import List, Dict, Any, Tuple
from sqlalchemy.orm import Session
import logging

from core.dto import P2POrderDTO, SpotPairDTO
from core.models import SpotSnapshot
from data_storage.repositories import P2PRepository, SpotRepository
from data_storage.database import SessionLocal

logger = logging.getLogger(__name__)

class P2PSnapshotManager:
    """Manager for creating snapshots of P2P market data."""
    
    def __init__(self, db_session: Session, collectors: List, assets: List[str]):
        self.repository = P2PRepository(db_session)
        self.collectors = collectors
        self.assets = assets

    def create_snapshot_concurrent(self) -> Dict[str, Any]:
        """Create a new snapshot using concurrent execution."""
        timestamp = datetime.now()
        snapshot = self.repository.create_snapshot(timestamp)
        results = {"snapshot_id": snapshot.id, "timestamp": timestamp,
                  "orders_by_exchange": {}, "total_orders": 0}

        # Function to collect from a single exchange
        def collect_from_exchange(collector):
            exchange_name = collector.__class__.__name__.replace("Collector", "")

            logger.info(f"Collecting P2POrders from {exchange_name}...")

            orders_count = 0

            for asset in self.assets:
                try:
                    p2p_orders = collector.fetch_p2p_orders(asset)
                    self.repository.add_orders_batch_postgresql(snapshot, p2p_orders)
                    orders_count += len(p2p_orders)
                except Exception as e:
                    logger.error(f"Error while collecting P2POrders from {exchange_name}: {e}")

            logger.info(f"Collection orders from {exchange_name} finished successfully. Orders: {orders_count}")

            return exchange_name, orders_count


        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.collectors)) as executor:
            future_to_collector = {executor.submit(collect_from_exchange, collector):
                                       collector for collector in self.collectors}

            for future in concurrent.futures.as_completed(future_to_collector):
                try:
                    exchange_name, orders_count = future.result()
                    results["orders_by_exchange"][exchange_name] = orders_count
                    results["total_orders"] += orders_count
                except Exception as e:
                    logger.exception(f"Error processing collector results: {e}")

        return results

class SpotSnapshotManager:
    """Manager for creating snapshots of spot market data."""
    
    def __init__(self, db_session: Session, collectors: List, 
                base_assets: List[str], quote_assets: List[str]):
        self.repository = SpotRepository(db_session)
        self.collectors = collectors
        self.base_assets = base_assets
        self.quote_assets = quote_assets

    def create_snapshot_concurrent(self) -> Dict[str, Any]:
        """Create snapshot with concurrent data collection."""
        timestamp = datetime.now()
        snapshot = self.repository.create_snapshot(timestamp)
        self.repository.db.commit()

        snapshot_id = snapshot.id

        # Set up concurrent collection
        # Use ThreadPoolExecutor to parallelize collection across exchanges
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.collectors)) as executor:
            # Submit collection tasks for each exchange
            future_to_collector = {
                executor.submit(self._collect_from_exchange, collector, snapshot_id): collector
                for collector in self.collectors
            }

            # Process results as they complete
            results = {
                "snapshot_id": snapshot_id,
                "timestamp": timestamp,
                "pairs_by_exchange": {},
                "total_pairs": 0,
                "execution_time": 0
            }

            start_time = time.time()

            for future in concurrent.futures.as_completed(future_to_collector):
                try:
                    exchange_name, pairs_count = future.result()
                    results["pairs_by_exchange"][exchange_name] = pairs_count
                    results["total_pairs"] += pairs_count
                except Exception as e:
                    collector = future_to_collector[future]
                    exchange_name = collector.__class__.__name__.replace("Collector", "")
                    logger.exception(f"Error from {exchange_name}: {e}")
                    results["pairs_by_exchange"][exchange_name] = 0

            end_time = time.time()
            results["execution_time"] = end_time - start_time

            logger.info(f"Concurrent snapshot creation completed in {results["execution_time"]:.2f} seconds")

        return results

    def _collect_from_exchange(self, collector, snapshot_id) -> Tuple[str, int]:
        """Collect data from a single exchange."""
        exchange_name = collector.__class__.__name__.replace("Collector", "")
        pairs_count = 0

        logger.info(f"Collecting SpotPairs from {exchange_name}...")

        thread_session = SessionLocal()

        try:
            # Get the snapshot by ID (not passing the object between threads)
            snapshot = thread_session.query(SpotSnapshot).get(snapshot_id)

            if not snapshot:
                logger.error(f"Snapshot with ID {snapshot_id} not found")
                return exchange_name, 0

            # Fetch all pairs at once for efficiency
            pairs = collector.fetch_spot_pairs()

            # Filter pairs by configured assets
            # valid_pairs = [
            #     pair for pair in pairs
            #     if pair.base_asset_symbol in self.base_assets and
            #        pair.quote_asset_symbol in self.quote_assets
            # ]

            # Use PostgreSQL-optimized batch insertion
            if pairs:
                self.repository.add_pairs_batch_postgresql(snapshot, pairs)
                pairs_count = len(pairs)

            logger.info(f"Collected {pairs_count} pairs from {exchange_name}")
            return exchange_name, pairs_count

        except Exception as e:
            logger.error(f"Error collecting SpotPairs from {exchange_name}: {e}")
            # Try individual collection as fallback
            thread_session.rollback()
            return exchange_name, 0
        finally:
            # Close the thread's session
            thread_session.close()