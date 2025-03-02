from datetime import datetime
from data_storage.repositories import SnapshotRepository

class SnapshotManager:
    """Manager for creating snapshots of exchange data."""
    
    def __init__(self, db_session, collectors, assets):
        self.repository = SnapshotRepository(db_session)
        self.collectors = collectors
        self.assets = assets
    
    def create_snapshot(self):
        """Create a new snapshot of all exchange data."""
        timestamp = datetime.now()
        snapshot = self.repository.create_snapshot(timestamp)
        
        for collector in self.collectors:
            for asset in self.assets:
                # Collect P2P prices
                try:
                    p2p_prices = collector.fetch_p2p_prices(asset)
                    for price in p2p_prices:
                        self.repository.add_price_point(snapshot, price)
                except Exception as e:
                    print(f"Error collecting P2P prices from {collector.__class__.__name__}: {e}")
                
                # Collect exchange prices
                try:
                    exchange_price = collector.fetch_exchange_prices(asset)
                    if exchange_price:
                        self.repository.add_price_point(snapshot, exchange_price)
                except Exception as e:
                    print(f"Error collecting exchange prices from {collector.__class__.__name__}: {e}")
                
                # Collect available amounts
                for order_type in ['BUY', 'SELL']:
                    try:
                        amount = collector.fetch_available_amount(asset, order_type)
                        # Store the amount information
                    except Exception as e:
                        print(f"Error collecting available amount from {collector.__class__.__name__}: {e}")
        
        return snapshot