# data_storage/snapshots.py
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy.orm import Session

from core.dto import P2POrderDTO, SpotPairDTO
from data_storage.repositories import P2PRepository, SpotRepository

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
                    for order in p2p_orders:
                        self.repository.add_order(snapshot, order)
                        orders_count += 1
                except Exception as e:
                    print(f"Error collecting P2P orders from {collector.__class__.__name__} for {asset}: {e}")
            
            results["orders_by_exchange"][exchange_name] = orders_count
            total_orders += orders_count
        
        results["total_orders"] = total_orders
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