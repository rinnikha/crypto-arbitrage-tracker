"""
Main application class for the Crypto Arbitrage Tracker.
"""
import logging
import sys
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
import threading
import signal
import atexit

from sqlalchemy.orm import Session
from sqlalchemy import create_engine
from apscheduler.schedulers.background import BackgroundScheduler

from config.manager import ConfigManager
from core.errors import setup_logging, AppError
from data_storage.database import get_db
from core.models import Base
from data_storage.repositories import P2PRepository, SpotRepository
from data_storage.snapshot_managers import P2PSnapshotManager, SpotSnapshotManager
from data_collection.api_clients import BaseCollector
from api.app import create_api, run_api

logger = logging.getLogger(__name__)


class Application:
    """
    Main application class for the Crypto Arbitrage Tracker.

    This class is responsible for initializing and coordinating all components
    of the application.
    """

    def __init__(self, config: ConfigManager):
        """
        Initialize the application.

        Args:
            config: Configuration manager
        """
        self.config = config
        self.db_engine = None
        self.db_session = None
        self.scheduler = None
        self.collectors = []
        self.p2p_manager = None
        self.spot_manager = None
        self.api_app = None
        self.api_thread = None
        self.is_running = False
        self.initialized = False

    def initialize(self) -> None:
        """Initialize the application components."""
        logger.info("Initializing Crypto Arbitrage Tracker...")

        # Initialize database
        self._init_database()

        # Initialize caches
        self._init_caches()

        # Initialize collectors
        self._init_collectors()

        # Initialize repositories and snapshot managers
        self._init_repositories()

        # Initialize API
        self._init_api()

        # Initialize scheduler
        self._init_scheduler()

        # Register shutdown handler
        self._register_shutdown_handler()

        self.initialized = True
        logger.info("Application initialized successfully")

    def _init_database(self) -> None:
        """Initialize the database."""
        database_url = self.config.require('DATABASE_URL')
        logger.info(f"Initializing database: {database_url}")

        # Create engine
        engine = create_engine(
            database_url,
            pool_size=self.config.get_int('DB_POOL_SIZE', 10),
            max_overflow=self.config.get_int('DB_MAX_OVERFLOW', 20),
            pool_timeout=self.config.get_int('DB_POOL_TIMEOUT', 30),
            pool_recycle=self.config.get_int('DB_POOL_RECYCLE', 1800)
        )

        # Ensure tables exist (only in development)
        if self.config.get('ENVIRONMENT', 'development') == 'development':
            Base.metadata.create_all(bind=engine)

        # Create session
        self.db_engine = engine
        self.db_session = next(get_db(engine))

        # Seed database with initial data if in development mode
        if self.config.get('ENVIRONMENT', 'development') == 'development':
            from data_storage.seed import seed_database
            seed_database(self.db_session)

        logger.info("Database initialized")

    def _init_caches(self) -> None:
        """Initialize data caches."""
        logger.info("Initializing reference data caches...")

        from core.cache import initialize_caches, setup_cache_refresh

        # Initialize caches with initial data
        initialize_caches()

        # Set up periodic cache refresh
        self.cache_scheduler = setup_cache_refresh()

        logger.info("Reference data caches initialized")

    def _init_collectors(self) -> None:
        """Initialize data collectors."""
        logger.info("Initializing data collectors...")

        # Get exchange settings
        exchange_settings = self.config.get('EXCHANGE_SETTINGS', {})

        # Import collector classes
        from data_collection.api_clients import BinanceCollector
        from data_collection.api_clients import BitgetCollector
        from data_collection.api_clients import BybitCollector
        from data_collection.api_clients import MexcCollector
        from data_collection.api_clients import TonWalletCollector

        # Initialize collectors
        self.collectors = []

        # Binance collector
        if exchange_settings.get('binance', {}).get('enabled', True):
            binance_settings = exchange_settings.get('binance', {})
            self.collectors.append(BinanceCollector(
                api_key=binance_settings.get('api_key'),
                api_secret=binance_settings.get('api_secret')
            ))
            logger.info("Initialized Binance collector")

        # Bitget collector
        if exchange_settings.get('bitget', {}).get('enabled', True):
            bitget_settings = exchange_settings.get('bitget', {})
            self.collectors.append(BitgetCollector(
                api_key=bitget_settings.get('api_key'),
                api_secret=bitget_settings.get('api_secret'),
                passphrase=bitget_settings.get('passphrase')
            ))
            logger.info("Initialized Bitget collector")

        # Bybit collector
        if exchange_settings.get('bybit', {}).get('enabled', True):
            bybit_settings = exchange_settings.get('bybit', {})
            self.collectors.append(BybitCollector(
                api_key=bybit_settings.get('api_key'),
                api_secret=bybit_settings.get('api_secret')
            ))
            logger.info("Initialized Bybit collector")

        # MEXC collector
        if exchange_settings.get('mexc', {}).get('enabled', True):
            mexc_settings = exchange_settings.get('mexc', {})
            self.collectors.append(MexcCollector(
                api_key=mexc_settings.get('api_key'),
                api_secret=mexc_settings.get('api_secret')
            ))
            logger.info("Initialized MEXC collector")

        # TON Wallet collector
        # if exchange_settings.get('ton_wallet', {}).get('enabled', True):
        #     ton_settings = exchange_settings.get('ton_wallet', {})
        #     self.collectors.append(TonWalletCollector(
        #         api_token=ton_settings.get('api_token')
        #     ))
        #     logger.info("Initialized TON Wallet collector")

        logger.info(f"Initialized {len(self.collectors)} data collectors")

    def _init_repositories(self) -> None:
        """Initialize repositories and snapshot managers."""
        logger.info("Initializing repositories and snapshot managers...")

        # Get assets list
        assets = self.config.get('ASSETS', ["USDT", "BTC", "ETH"])

        # Initialize repositories
        p2p_repo = P2PRepository(self.db_session)
        spot_repo = SpotRepository(self.db_session)

        # Initialize snapshot managers
        self.p2p_manager = P2PSnapshotManager(
            db_session=self.db_session,
            collectors=self.collectors,
            assets=assets
        )

        self.spot_manager = SpotSnapshotManager(
            db_session=self.db_session,
            collectors=self.collectors,
            base_assets=assets,
            quote_assets=["USDT", "USD"]  # Quote assets to track
        )

        # Store repositories for API
        self.repositories = {
            "p2p_repo": p2p_repo,
            "spot_repo": spot_repo
        }

        logger.info("Repositories and snapshot managers initialized")

    def _init_api(self) -> None:
        """Initialize API."""
        logger.info("Initializing API...")

        # Create API app
        self.api_app = create_api(self.repositories)

        logger.info("API initialized")

    def _init_scheduler(self) -> None:
        """Initialize scheduler."""
        logger.info("Initializing scheduler...")

        # Create scheduler
        self.scheduler = BackgroundScheduler()

        # Get snapshot interval
        snapshot_interval = self.config.get_int('SNAPSHOT_INTERVAL_MINUTES', 5)

        # Schedule P2P snapshot creation
        self.scheduler.add_job(
            lambda: self._create_p2p_snapshot(),
            'interval',
            minutes=snapshot_interval,
            id='p2p_snapshot'
        )

        # Schedule spot snapshot creation
        self.scheduler.add_job(
            lambda: self._create_spot_snapshot(),
            'interval',
            minutes=snapshot_interval,
            id='spot_snapshot',
            # Offset slightly to avoid running both at the same time
            next_run_time=datetime.now() + timedelta(seconds=30)
        )

        logger.info(f"Scheduler initialized with {snapshot_interval} minute interval")

    def _register_shutdown_handler(self) -> None:
        """Register shutdown handler."""

        def shutdown_handler(signal=None, frame=None):
            """Shutdown handler for graceful shutdown."""
            logger.info("Shutting down...")
            self.shutdown()

        # Register signal handlers
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, shutdown_handler)

        # Register atexit handler
        atexit.register(shutdown_handler)

    def start(self) -> None:
        """Start the application."""
        if not self.initialized:
            self.initialize()

        logger.info("Starting Crypto Arbitrage Tracker...")

        # Take initial snapshots
        self._take_initial_snapshots()

        # Start scheduler
        self.scheduler.start()
        logger.info("Scheduler started")

        # Start API in a separate thread
        api_host = self.config.get('API_HOST', '0.0.0.0')
        api_port = self.config.get_int('API_PORT', 5000)

        self.api_thread = threading.Thread(
            target=run_api,
            args=(self.api_app, api_host, api_port),
            daemon=True
        )
        self.api_thread.start()
        logger.info(f"API started on {api_host}:{api_port}")

        self.is_running = True
        logger.info("Crypto Arbitrage Tracker started")

    def _take_initial_snapshots(self) -> None:
        """Take initial snapshots."""
        logger.info("Taking initial snapshots...")

        try:
            # Try to take initial snapshots
            p2p_result = self._create_p2p_snapshot()
            spot_result = self._create_spot_snapshot()

            logger.info(
                f"Initial snapshots completed: "
                f"P2P ({p2p_result['snapshot_id']}): {p2p_result['total_orders']} orders, "
                f"Spot ({spot_result['snapshot_id']}): {spot_result['total_pairs']} pairs"
            )
        except Exception as e:
            logger.error(f"Error taking initial snapshots: {e}")

    def _create_p2p_snapshot(self) -> Dict[str, Any]:
        """
        Create a new P2P snapshot.

        Returns:
            Snapshot result
        """
        try:
            logger.info("Creating P2P snapshot...")
            result = self.p2p_manager.create_snapshot_concurrent()
            logger.info(f"Created P2P snapshot {result['snapshot_id']} with {result['total_orders']} orders")
            return result
        except Exception as e:
            logger.error(f"Error creating P2P snapshot: {e}")
            raise

    def _create_spot_snapshot(self) -> Dict[str, Any]:
        """
        Create a new spot snapshot.

        Returns:
            Snapshot result
        """
        try:
            logger.info("Creating spot snapshot...")
            result = self.spot_manager.create_snapshot_concurrent()
            logger.info(f"Created spot snapshot {result['snapshot_id']} with {result['total_pairs']} pairs")
            return result
        except Exception as e:
            logger.error(f"Error creating spot snapshot: {e}")
            raise

    def shutdown(self) -> None:
        """Shutdown the application."""
        if not self.is_running:
            return

        logger.info("Shutting down Crypto Arbitrage Tracker...")

        # Shutdown cache scheduler if exists
        if hasattr(self, 'cache_scheduler') and self.cache_scheduler:
            self.cache_scheduler.shutdown()
            logger.info("Cache scheduler stopped")

        # Shutdown scheduler
        if self.scheduler:
            self.scheduler.shutdown()
            logger.info("Scheduler stopped")

        # Close database session
        if self.db_session:
            self.db_session.close()
            logger.info("Database session closed")

        self.is_running = False
        logger.info("Crypto Arbitrage Tracker shut down")

    def run(self) -> None:
        """Run the application."""
        try:
            # Start the application
            self.start()

            # Keep main thread alive
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            # Handle Ctrl+C
            logger.info("Received keyboard interrupt")
        except Exception as e:
            # Handle unexpected exceptions
            logger.exception(f"Unexpected error: {e}")
        finally:
            # Ensure shutdown
            self.shutdown()


def main() -> None:
    """Main entry point."""
    try:
        # Set up logging
        setup_logging(
            level=ConfigManager().get('LOG_LEVEL', 'INFO'),
            log_file=ConfigManager().get('LOG_FILE')
        )

        # Create and run application
        app = Application(ConfigManager())
        app.run()
    except AppError as e:
        logger.error(f"Application error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Unhandled exception: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()