"""
Database seeding module for initial data population.
"""
import logging
from typing import List, Dict, Any

from sqlalchemy.orm import Session

from core.models import Exchange, Asset, Fiat

logger = logging.getLogger(__name__)

# Exchange data
EXCHANGES = [
    {"name": "Binance", "base_url": "https://api.binance.com",
     "p2p_url": "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
     "fiat_currencies": ["USD", "EUR", "RUB", "UZS", "KZT"]},
    {"name": "Bybit", "base_url": "https://api.bybit.com",
     "p2p_url": "https://api.bybit.com/v5/spot/c2c/order-book",
     "fiat_currencies": ["USD", "EUR", "RUB"]},
    {"name": "MEXC", "base_url": "https://api.mexc.com",
     "p2p_url": "https://otc.mexc.com/api",
     "fiat_currencies": ["USD", "RUB"]},
    {"name": "Bitget", "base_url": "https://api.bitget.com",
     "p2p_url": "https://api.bitget.com/api/spot/v1/p2p/merchant/advertise/list",
     "fiat_currencies": ["USD", "EUR", "RUB"]},
    {"name": "TON P2P", "base_url": "https://fragment.com",
     "p2p_url": "https://fragment.com/exchange/TONCOIN",
     "fiat_currencies": ["USD"]}
]

# Fiat currencies data - focusing on CIS countries
FIATS = [
    {"code": "USD", "name": "US Dollar"},
    {"code": "EUR", "name": "Euro"},
    {"code": "RUB", "name": "Russian Ruble"},
    {"code": "UZS", "name": "Uzbekistani Som"},
    {"code": "KZT", "name": "Kazakhstani Tenge"},
    {"code": "TRY", "name": "Turkish Lira"},
    {"code": "BYN", "name": "Belarusian Ruble"},
    {"code": "UAH", "name": "Ukrainian Hryvnia"},
    {"code": "AZN", "name": "Azerbaijani Manat"},
    {"code": "AMD", "name": "Armenian Dram"},
    {"code": "GEL", "name": "Georgian Lari"},
    {"code": "MDL", "name": "Moldovan Leu"},
    {"code": "TJS", "name": "Tajikistani Somoni"},
    {"code": "TMT", "name": "Turkmenistani Manat"},
    {"code": "KGS", "name": "Kyrgyzstani Som"}
]

# Asset data - basic cryptocurrencies
ASSETS = [
    {"symbol": "BTC", "name": "Bitcoin"},
    {"symbol": "ETH", "name": "Ethereum"},
    {"symbol": "USDT", "name": "Tether"},
    {"symbol": "USDC", "name": "USD Coin"},
    {"symbol": "TON", "name": "Toncoin"},
    {"symbol": "BNB", "name": "Binance Coin"},
    {"symbol": "XRP", "name": "Ripple"},
    {"symbol": "SOL", "name": "Solana"},
    {"symbol": "ADA", "name": "Cardano"},
    {"symbol": "DOGE", "name": "Dogecoin"}
]


def seed_exchanges(session: Session) -> int:
    """
    Seed the exchanges table.

    Args:
        session: SQLAlchemy database session

    Returns:
        Number of exchanges inserted
    """
    # Check if table already has data
    existing_count = session.query(Exchange).count()
    if existing_count > 0:
        logger.info(f"Exchanges table already has {existing_count} records, skipping seeding")
        return 0

    logger.info("Seeding exchanges table")
    count = 0

    # Add each exchange
    for exchange_data in EXCHANGES:
        exchange = Exchange(**exchange_data)
        session.add(exchange)
        count += 1

    # Flush to assign IDs but don't commit yet
    session.flush()
    logger.info(f"Added {count} exchanges")
    return count


def seed_fiats(session: Session) -> int:
    """
    Seed the fiats table.

    Args:
        session: SQLAlchemy database session

    Returns:
        Number of fiats inserted
    """
    # Check if table already has data
    existing_count = session.query(Fiat).count()
    if existing_count > 0:
        logger.info(f"Fiats table already has {existing_count} records, skipping seeding")
        return 0

    logger.info("Seeding fiats table")
    count = 0

    # Add each fiat currency
    for fiat_data in FIATS:
        fiat = Fiat(**fiat_data)
        session.add(fiat)
        count += 1

    # Flush to assign IDs but don't commit yet
    session.flush()
    logger.info(f"Added {count} fiat currencies")
    return count


def seed_assets(session: Session) -> int:
    """
    Seed the assets table.

    Args:
        session: SQLAlchemy database session

    Returns:
        Number of assets inserted
    """
    # Check if table already has data
    existing_count = session.query(Asset).count()
    if existing_count > 0:
        logger.info(f"Assets table already has {existing_count} records, skipping seeding")
        return 0

    logger.info("Seeding assets table")
    count = 0

    # Add each asset
    for asset_data in ASSETS:
        asset = Asset(**asset_data)
        session.add(asset)
        count += 1

    # Flush to assign IDs but don't commit yet
    session.flush()
    logger.info(f"Added {count} assets")
    return count


def seed_database(session: Session) -> Dict[str, int]:
    """
    Seed the database with initial data.

    Args:
        session: SQLAlchemy database session

    Returns:
        Dictionary with count of items inserted in each table
    """
    try:
        logger.info("Starting database seeding")

        # Seed in order to handle dependencies
        exchanges_count = seed_exchanges(session)
        fiats_count = seed_fiats(session)
        assets_count = seed_assets(session)

        # Commit the transaction
        session.commit()

        logger.info("Database seeding completed successfully")
        return {
            "exchanges": exchanges_count,
            "fiats": fiats_count,
            "assets": assets_count
        }
    except Exception as e:
        # Roll back on error
        session.rollback()
        logger.error(f"Error during database seeding: {e}")
        raise