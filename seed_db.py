"""
Standalone script to seed the database with initial data.
"""
import logging
import sys

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from config.manager import ConfigManager
from core.models import Base
from core.errors import setup_logging, AppError, DatabaseError
from data_storage.seed import seed_database


def main():
    """Main entry point for the seeding script."""
    try:
        # Load configuration
        config = ConfigManager()

        # Set up logging
        setup_logging(
            level=config.get('LOG_LEVEL', 'INFO'),
            log_file=config.get('LOG_FILE'),
            log_format=config.get('LOG_FORMAT')
        )

        logger = logging.getLogger(__name__)
        logger.info("Starting database seeding script")

        # Get database URL
        database_url = config.require('DATABASE_URL')
        logger.info(f"Connecting to database: {database_url}")

        # Create engine and session
        engine = create_engine(database_url)
        Session = sessionmaker(bind=engine)
        session = Session()

        try:
            # Create tables if they don't exist
            Base.metadata.create_all(bind=engine)

            # Seed the database
            result = seed_database(session)

            # Log results
            logger.info(f"Seeding results: {result}")
            logger.info("Database seeding completed successfully")

        except Exception as e:
            logger.exception(f"Error during database seeding: {e}")
            session.rollback()
            raise DatabaseError(f"Database seeding failed: {e}")
        finally:
            session.close()

    except AppError as e:
        logging.error(f"Application error: {e}")
        sys.exit(1)
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()