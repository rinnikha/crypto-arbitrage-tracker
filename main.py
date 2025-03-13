# main.py
import sys
import logging
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, NullPool, QueuePool
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta

import threading

from api.app import create_api, run_api
from data_storage.database import get_db
from core.models import Base
from data_storage.snapshots import P2PSnapshotManager, SpotSnapshotManager
from data_collection.api_clients.binance import BinanceCollector
from data_collection.api_clients.bitget import BitgetCollector
from data_collection.api_clients.bybit import BybitCollector
from data_collection.api_clients.mexc import MexcCollector
from data_collection.api_clients.ton_wallet import TonWalletCollector
from data_storage.repositories import P2PRepository, SpotRepository
from config.exchanges import EXCHANGE_SETTINGS, ASSETS
from config.settings import DATABASE_URL, API_HOST, API_PORT, SNAPSHOT_INTERVAL_MINUTES
# from ui.app import create_app

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("crypto_arbitrage.log")
    ]
)

logger = logging.getLogger(__name__)

def init_database():
    """Initialize the database."""
    logger.info("Initializing database...")
    engine = create_engine(
        DATABASE_URL,
        poolclass=QueuePool,
        pool_size=10,
        max_overflow=20,
        pool_timeout=30,
        pool_recycle=1800
    )
    Base.metadata.create_all(bind=engine)
    return engine

def create_p2p_snapshot(p2p_manager):
    """Job to create a new P2P snapshot."""
    try:
        logger.info(f"Creating P2P snapshot at {datetime.now()}")
        result = p2p_manager.create_snapshot_concurrent()
        logger.info(f"Created P2P snapshot {result['snapshot_id']} with {result['total_orders']} orders")
    except Exception as e:
        logger.exception(f"Error creating P2P snapshot: {e}")

def create_spot_snapshot(spot_manager):
    """Job to create a new spot snapshot."""
    try:
        logger.info(f"Creating spot snapshot at {datetime.now()}")
        result = spot_manager.create_snapshot_concurrent()
        logger.info(f"Created spot snapshot {result['snapshot_id']} with {result['total_pairs']} pairs")
    except Exception as e:
        logger.exception(f"Error creating spot snapshot: {e}")

def setup_scheduler(p2p_manager, spot_manager):
    """Set up the scheduler with periodic jobs."""
    scheduler = BackgroundScheduler()
    
    # Schedule P2P snapshot creation
    scheduler.add_job(
        lambda: create_p2p_snapshot(p2p_manager), 
        'interval',
        minutes=SNAPSHOT_INTERVAL_MINUTES,
        id='p2p_snapshot'
    )
    
    # Schedule spot snapshot creation
    scheduler.add_job(
        lambda: create_spot_snapshot(spot_manager), 
        'interval', 
        minutes=SNAPSHOT_INTERVAL_MINUTES,
        id='spot_snapshot',
        # Offset slightly to avoid running both at the same time
        next_run_time=datetime.now() + timedelta(seconds=30)
    )
    
    return scheduler

def main():
    """Main entry point."""
    logger.info("Initializing Crypto Arbitrage Tracker...")

    # Initialize the database
    engine = init_database()

    # Create a session
    session = next(get_db(engine))
    
    # Initialize collectors
    collectors = []
    
    # Binance collector
    if EXCHANGE_SETTINGS["binance"]["enabled"]:
        collectors.append(BinanceCollector(
            api_key=EXCHANGE_SETTINGS["binance"]["api_key"],
            api_secret=EXCHANGE_SETTINGS["binance"]["api_secret"]
        ))
    
    # Bitget collector
    if EXCHANGE_SETTINGS["bitget"]["enabled"]:
        collectors.append(BitgetCollector(
            api_key=EXCHANGE_SETTINGS["bitget"]["api_key"],
            api_secret=EXCHANGE_SETTINGS["bitget"]["api_secret"],
            passphrase=EXCHANGE_SETTINGS["bitget"]["passphrase"]
        ))
    
    # Bybit collector
    if EXCHANGE_SETTINGS["bybit"]["enabled"]:
        
        
        collectors.append(BybitCollector(
            api_key=EXCHANGE_SETTINGS["bybit"]["api_key"],
            api_secret=EXCHANGE_SETTINGS["bybit"]["api_secret"]
        ))

        
    
    # MEXC collector
    if EXCHANGE_SETTINGS["mexc"]["enabled"]:
        

        collectors.append(MexcCollector(
            api_key=EXCHANGE_SETTINGS["mexc"]["api_key"],
            api_secret=EXCHANGE_SETTINGS["mexc"]["api_secret"]
        ))
    
    # TON Wallet collector
    # if EXCHANGE_SETTINGS["ton_wallet"]["enabled"]:
    #     client = TonWalletCollector(api_token=EXCHANGE_SETTINGS["ton_wallet"]["api_token"])
    #     client.fetch_p2p_orders(ASSETS)
    #
    #     collectors.append(TonWalletCollector(
    #         api_token=EXCHANGE_SETTINGS["ton_wallet"]["api_token"]
    #     ))


    
    # Repositories
    p2p_repo = P2PRepository(session)
    spot_repo = SpotRepository(session)
    
    # Initialize snapshot managers
    p2p_manager = P2PSnapshotManager(
        db_session=session,
        collectors=collectors,
        assets=ASSETS
    )
    
    spot_manager = SpotSnapshotManager(
        db_session=session,
        collectors=collectors,
        base_assets=ASSETS,
        quote_assets=["USDT", "USD"]  # Quote assets to track
    )

    
    # Set up and start the scheduler
    scheduler = setup_scheduler(p2p_manager, spot_manager)
    scheduler.start()

    # Create repositories for API
    repositories = {
        "p2p_repo": p2p_repo,
        "spot_repo": spot_repo
    }

    # Create API app
    api = create_api(repositories)
    
    
    try:
        # Take initial snapshots
        create_p2p_snapshot(p2p_manager)
        create_spot_snapshot(spot_manager)
        
        run_api(api, host=API_HOST, port=API_PORT)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        scheduler.shutdown()
        sys.exit(0)
    except Exception as e:
        logger.exception(f"Error running application: {e}")
        scheduler.shutdown()
        sys.exit(1)

if __name__ == "__main__":
    main()