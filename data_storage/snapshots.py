# data_storage/snapshots.py
from datetime import datetime
import concurrent.futures
from typing import List, Dict, Any, Tuple
from sqlalchemy.orm import Session
import logging

from core.dto import P2POrderDTO, SpotPairDTO
from data_storage.repositories import P2PRepository, SpotRepository

logger = logging.getLogger(__name__)

class P2PSnapshotManager:
    """Manager for creating snapshots of P2P market data."""
    
    def __init__(self, db_session: Session, collectors: List, assets: List[str]):
        self.repository = P2PRepository(db_session)
        self.collectors = collectors
        self.assets = assets
    
    def create_snapshot(self) -> Dict[str, Any]:
        """
        Create a new snapshot of P2P market data.
        
        Returns:
            Dictionary with snapshot info
        """
        timestamp = datetime.now()
        snapshot = self.repository.create_snapshot(timestamp)
        
        total_orders = 0
        results = {
            "snapshot_id": snapshot.id,
            "timestamp": timestamp,
            "orders_by_exchange": {}
        }
        
        for collector in self.collectors:
            exchange_name = collector.__class__.__name__.replace("Collector", "")
            orders_count = 0
            
            for asset in self.assets:
                # Collect P2P orders
                try:
                    p2p_orders = collector.fetch_p2p_orders(asset)
                    self.repository.add_orders_batch(snapshot, p2p_orders)
                    orders_count += len(p2p_orders)
                except Exception as e:
                    logger.exception(f"Error collecting P2P orders from {collector.__class__.__name__} for {asset}: {e}")
            
            results["orders_by_exchange"][exchange_name] = orders_count
            total_orders += orders_count
        
        results["total_orders"] = total_orders
        return results

    def create_snapshot_concurrent(self) -> Dict[str, Any]:
        """Create a new snapshot using concurrent execution."""
        timestamp = datetime.now()
        snapshot = self.repository.create_snapshot(timestamp)
        results = {"snapshot_id": snapshot.id, "timestamp": timestamp,
                  "orders_by_exchange": {}, "total_orders": 0}

        # Function to collect from a single exchange
        def collect_from_exchange(collector):
            exchange_name = collector.__class__.__name__.replace("Collector", "")
            orders_count = 0

            for asset in self.assets:
                try:
                    p2p_orders = collector.fetch_p2p_orders(asset)
                    self.repository.add_orders_batch(snapshot, p2p_orders)
                    orders_count += len(p2p_orders)
                except Exception as e:
                    logger.error(f"Error collecting orders: {e}")

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
    
    def create_snapshot(self) -> Dict[str, Any]:
        """
        Create a new snapshot of spot market data.
        
        Returns:
            Dictionary with snapshot info
        """
        timestamp = datetime.now()
        snapshot = self.repository.create_snapshot(timestamp)
        
        total_pairs = 0
        results = {
            "snapshot_id": snapshot.id,
            "timestamp": timestamp,
            "pairs_by_exchange": {}
        }
        
        for collector in self.collectors:
            exchange_name = collector.__class__.__name__.replace("Collector", "")
            pairs_count = 0
            
            # Option 1: Fetch all pairs at once
            try:
                all_pairs = collector.fetch_spot_pairs()
                for pair in all_pairs:
                    # Filter by configured assets
                    if pair.base_asset_symbol in self.base_assets and \
                       pair.quote_asset_symbol in self.quote_assets:
                        self.repository.add_pair(snapshot, pair)
                        pairs_count += 1
            except Exception as e:
                print(f"Error collecting all spot pairs from {collector.__class__.__name__}: {e}")
                
                # Option 2: Fetch specific pairs if all-at-once fails
                for base_asset in self.base_assets:
                    for quote_asset in self.quote_assets:
                        try:
                            specific_pairs = collector.fetch_spot_pairs(
                                base_asset=base_asset,
                                quote_asset=quote_asset
                            )
                            for pair in specific_pairs:
                                self.repository.add_pair(snapshot, pair)
                                pairs_count += 1
                        except Exception as e:
                            print(f"Error collecting spot pair {base_asset}/{quote_asset} from {collector.__class__.__name__}: {e}")
            
            results["pairs_by_exchange"][exchange_name] = pairs_count
            total_pairs += pairs_count
        
        results["total_pairs"] = total_pairs
        return results

    def create_snapshot_concurrent(self) -> Dict[str, Any]:
        """
        Create a new snapshot of spot market data using concurrent execution.

        This method collects spot pair data from multiple exchanges in parallel,
        substantially reducing the time required to create a complete snapshot
        compared to the sequential approach.

        Returns:
            Dictionary with snapshot info
        """
        timestamp = datetime.now()
        snapshot = self.repository.create_snapshot(timestamp)

        results = {
            "snapshot_id": snapshot.id,
            "timestamp": timestamp,
            "pairs_by_exchange": {},
            "total_pairs": 0,
            "execution_time": 0
        }

        start_time = datetime.now()

        def collect_from_exchange(collector) -> Tuple[str, int]:
            """
            Collect spot pairs from a single exchange.

            Args:
                collector: The collector instance for a specific exchange

            Returns:
                Tuple containing exchange name and number of pairs collected
            """
            exchange_name = collector.__class__.__name__.replace("Collector", "")
            pairs_count = 0

            try:
                # First attempt: fetch all pairs at once (more efficient)
                all_pairs = collector.fetch_spot_pairs()
                valid_pairs = []

                # Filter pairs by configured assets
                for pair in all_pairs:
                    if (pair.base_asset_symbol in self.base_assets and
                        pair.quote_asset_symbol in self.quote_assets):
                        valid_pairs.append(pair)

                # Batch add pairs to improve database performance
                if valid_pairs:
                    self.repository.add_pairs_batch(snapshot, valid_pairs)
                    pairs_count = len(valid_pairs)

            except Exception as e:
                logger.warning(f"Error collecting all spot pairs from {exchange_name}: {e}")
                logger.info(f"Falling back to individual pair collection for {exchange_name}")

                # Option 2: Fetch specific pairs if all-at-once fails
                for base_asset in self.base_assets:
                    for quote_asset in self.quote_assets:
                        try:
                            specific_pairs = collector.fetch_spot_pairs(
                                base_asset=base_asset,
                                quote_asset=quote_asset
                            )

                            if specific_pairs:
                                # Add pairs for this specific combination
                                self.repository.add_pairs_batch(snapshot, specific_pairs)
                                pairs_count += len(specific_pairs)

                        except Exception as pair_error:
                            logger.error(
                                f"Error collecting spot pair {base_asset}/{quote_asset} "
                                f"from {exchange_name}: {pair_error}"
                            )

            return exchange_name, pairs_count

        # Perform concurrent collection from all exchanges
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.collectors)) as executor:
            # Submit all collector tasks
            future_to_collector = {
                executor.submit(collect_from_exchange, collector): collector
                for collector in self.collectors
            }

            # Process results as they complete
            for future in concurrent.futures.as_completed(future_to_collector):
                try:
                    exchange_name, pairs_count = future.result()
                    results["pairs_by_exchange"][exchange_name] = pairs_count
                    results["total_pairs"] += pairs_count
                    logger.info(f"Collected {pairs_count} pairs from {exchange_name}")
                except Exception as exc:
                    collector = future_to_collector[future]
                    exchange_name = collector.__class__.__name__.replace("Collector", "")
                    logger.exception(f"Collector {exchange_name} generated an exception: {exc}")
                    results["pairs_by_exchange"][exchange_name] = 0

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        results["execution_time"] = execution_time

        logger.info(f"Concurrent snapshot creation completed in {execution_time:.2f} seconds")
        return results