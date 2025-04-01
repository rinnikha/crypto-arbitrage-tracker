#!/usr/bin/env python
"""
Script to clear all data from the database and reseed with initial data.
This is a destructive operation and should be used with caution.
"""
import os
import sys
import logging
import argparse
from datetime import datetime

from sqlalchemy import text

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('db_reset')


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description='Reset the database by deleting all data and reseeding'
    )
    parser.add_argument('--force', '-f', action='store_true',
                        help='Skip confirmation prompt')
    parser.add_argument('--seed', '-s', action='store_true',
                        help='Seed the database with initial data after reset')
    parser.add_argument('--alembic', '-a', action='store_true',
                        help='Use Alembic to reset the database (reinstalls migrations)')
    parser.add_argument('--env', '-e', choices=['development', 'testing', 'production'],
                        default='development',
                        help='Environment to run in (default: development)')
    parser.add_argument('--truncate', '-t', action='store_true',
                        help='Use TRUNCATE CASCADE instead of DELETE (faster but more aggressive)')
    return parser.parse_args()


def confirm_reset(force=False):
    """Confirm database reset with the user unless force flag is used."""
    if force:
        return True

    print("\n⚠️  WARNING: This will DELETE ALL DATA from the database! ⚠️\n")
    print("This action cannot be undone.")
    confirmation = input("Type 'RESET' to confirm: ")

    return confirmation == "RESET"


def reset_sequences(connection):
    """Reset all sequences (auto-increment counters) to start from 1."""
    logger.info("Resetting all sequences to start from 1")

    # Get all sequences in the database
    seq_query = text("""
        SELECT sequencename 
        FROM pg_sequences 
        WHERE schemaname = 'public'
    """)

    sequences = [row[0] for row in connection.execute(seq_query)]
    logger.info(f"Found {len(sequences)} sequences to reset")

    # Reset each sequence to start from 1
    for seq_name in sequences:
        try:
            # The 'false' parameter is critical - it means the next value will be 1 (not 2)
            connection.execute(text(f"SELECT setval('public.{seq_name}', 1, false)"))
            logger.info(f"Reset sequence: {seq_name}")
        except Exception as e:
            logger.warning(f"Failed to reset sequence {seq_name}: {e}")


def reset_with_sqlalchemy(use_truncate=False):
    """
    Reset the database using SQLAlchemy by truncating or deleting all tables.
    """
    try:
        # Import database components
        from sqlalchemy import create_engine, inspect, MetaData, text
        from config.manager import get_config

        # Get database URL
        config = get_config()
        database_url = config.require('DATABASE_URL')
        logger.info(f"Connecting to database: {database_url}")

        # Create engine and inspector
        engine = create_engine(database_url)
        inspector = inspect(engine)
        connection = engine.connect()

        # Begin transaction
        trans = connection.begin()

        try:
            # Get all table names
            table_names = inspector.get_table_names()
            logger.info(f"Found {len(table_names)} tables: {', '.join(table_names)}")

            if use_truncate:
                # Method 1: TRUNCATE CASCADE (fastest but most aggressive)
                logger.info("Using TRUNCATE CASCADE to clear all tables")

                # PostgreSQL-specific: disable triggers temporarily
                connection.execute(text("SET session_replication_role = 'replica';"))

                # Truncate all tables with CASCADE
                tables_str = ", ".join(table_names)
                if tables_str:
                    connection.execute(text(f"TRUNCATE TABLE {tables_str} CASCADE;"))

                # Re-enable triggers
                connection.execute(text("SET session_replication_role = 'origin';"))

            else:
                # Method 2: Temporarily disable foreign key constraints
                logger.info("Temporarily disabling foreign key constraints")
                connection.execute(text("SET session_replication_role = 'replica';"))

                # Delete from each table
                for table in table_names:
                    logger.info(f"Deleting all data from table: {table}")
                    connection.execute(text(f"DELETE FROM {table}"))

                # Re-enable foreign key constraints
                connection.execute(text("SET session_replication_role = 'origin';"))

            # Reset all sequences to start from 1
            reset_sequences(connection)

            # Commit transaction
            trans.commit()
            logger.info("All data deleted successfully and sequences reset")

        except Exception as e:
            # Rollback on error
            trans.rollback()
            logger.error(f"Error during database reset: {e}")
            raise
        finally:
            # Close connection
            connection.close()

        return True

    except Exception as e:
        logger.error(f"Failed to reset database: {e}")
        return False


def reset_with_alembic():
    """
    Reset the database using Alembic by downgrading to base and upgrading to head.
    """
    try:
        import subprocess

        # Step 1: Downgrade to base
        logger.info("Downgrading all migrations to base")
        result = subprocess.run(["alembic", "downgrade", "base"],
                                capture_output=True, text=True, check=True)
        if result.returncode != 0:
            logger.error(f"Alembic downgrade failed: {result.stderr}")
            return False

        logger.info("Database downgraded successfully")

        # Step 2: Upgrade to head
        logger.info("Upgrading all migrations to head")
        result = subprocess.run(["alembic", "upgrade", "head"],
                                capture_output=True, text=True, check=True)
        if result.returncode != 0:
            logger.error(f"Alembic upgrade failed: {result.stderr}")
            return False

        logger.info("Database upgraded successfully")
        return True

    except Exception as e:
        logger.error(f"Failed to reset database with Alembic: {e}")
        return False


def seed_database():
    """Seed the database with initial data."""
    try:
        logger.info("Seeding database with initial data")

        # Try to use the seed module if available
        try:
            from data_storage.seed import seed_database as run_seeding
            from sqlalchemy.orm import Session
            from data_storage.database import get_db, get_engine

            engine = get_engine()
            session = next(get_db(engine))

            result = run_seeding(session)
            logger.info(f"Database seeded successfully: {result}")
            return True

        except ImportError:
            # If the seed module is not available, try to run the seed script
            logger.info("Seed module not found, trying to run seed script")

            if os.path.exists("seed_db.py"):
                import subprocess
                result = subprocess.run(["python", "seed_db.py"],
                                        capture_output=True, text=True, check=True)
                if result.returncode != 0:
                    logger.error(f"Seed script failed: {result.stderr}")
                    return False

                logger.info("Database seeded via script successfully")
                return True
            else:
                logger.error("No seeding mechanism found")
                return False

    except Exception as e:
        logger.error(f"Failed to seed database: {e}")
        return False


def main():
    """Main entry point."""
    args = parse_args()

    # Set environment
    os.environ["ENVIRONMENT"] = args.env
    logger.info(f"Running in {args.env} environment")

    # Confirm reset
    if not confirm_reset(args.force):
        logger.info("Database reset cancelled")
        return

    # Reset database
    start_time = datetime.now()
    logger.info(f"Starting database reset at {start_time}")

    if args.alembic:
        success = reset_with_alembic()
    else:
        success = reset_with_sqlalchemy(use_truncate=args.truncate)

    if not success:
        logger.error("Database reset failed")
        sys.exit(1)

    # Seed database if requested
    if args.seed and success:
        seed_success = seed_database()
        if not seed_success:
            logger.error("Database seeding failed")
            sys.exit(1)

    # Log completion
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    logger.info(f"Database reset completed in {duration:.2f} seconds")

    print("\n✅ Database reset completed successfully!")
    if args.seed:
        print("✅ Database seeded with initial data")

    print("\nThe database is now clean and ready to use.")


if __name__ == "__main__":
    main()