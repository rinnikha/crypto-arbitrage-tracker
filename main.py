"""
Main entry point for the Crypto Arbitrage Tracker application.
"""
import sys
import logging

from config.manager import ConfigManager
from core.errors import setup_logging, AppError
from app import Application


def main() -> None:
    """Main entry point."""
    try:
        # Load configuration
        config = ConfigManager()

        # Set up logging
        setup_logging(
            level=config.get('LOG_LEVEL', 'INFO'),
            log_file=config.get('LOG_FILE'),
            log_format=config.get('LOG_FORMAT')
        )

        # Get logger
        logger = logging.getLogger(__name__)
        logger.info("Starting Crypto Arbitrage Tracker...")

        # Create and run application
        app = Application(config)
        app.run()

        # Exit with success
        sys.exit(0)
    except AppError as e:
        # Log application error
        logging.error(f"Application error: {e}")
        sys.exit(1)
    except Exception as e:
        # Log unexpected error
        logging.exception(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()