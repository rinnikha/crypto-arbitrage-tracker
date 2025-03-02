import sys
from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from data_storage.database import get_db
from core.models import Base
from scheduler.jobs import setup_scheduler
from data_storage.snapshots import SnapshotManager
from data_collection.api_clients.binance import BinanceCollector
from data_collection.api_clients.bitget import BitgetCollector
# Import other collectors
from ui.app import app as dash_app

def init_database():
    """Initialize the database."""
    engine = create_engine('sqlite:///crypto_arbitrage.db')
    Base.metadata.create_all(bind=engine)
    return engine

def main():
    """Main entry point."""
    print("Initializing Crypto Arbitrage Tracker...")
    
    # Initialize the database
    engine = init_database()
    
    # Create a session
    db = next(get_db(engine))
    
    # Initialize collectors
    collectors = [
        BinanceCollector(),
        BitgetCollector(),
        # Add other collectors
    ]
    
    # Define assets to track
    assets = ["USDT"]  # Can be expanded to other assets

    # binance_client = BinanceCollector()
    # binance_client.fetch_p2p_prices(assets[0])

    bitget_client = BitgetCollector()
    bitget_client.fetch_p2p_prices(assets[0])

    
    # Initialize snapshot manager
    snapshot_manager = SnapshotManager(db, collectors, assets)
    # snapshot_manager.create_snapshot()
    
    # Set up and start the scheduler
    # scheduler = setup_scheduler(snapshot_manager)
    # scheduler.start()
    
    try:
        # Run the UI
        dash_app.run_server(debug=True)
    except KeyboardInterrupt:
        print("Shutting down...")
        # scheduler.shutdown()
        sys.exit(0)

if __name__ == "__main__":
    main()