"""
Module for managing cache initialization and periodic refreshing.
"""
import logging
from typing import Dict, Any
from datetime import datetime, timedelta

from apscheduler.schedulers.background import BackgroundScheduler
from core.cache.reference_data import SymbolInfoCache, PaymentMethodCache

logger = logging.getLogger(__name__)

# List of exchanges to pre-cache
EXCHANGES_TO_CACHE = ["binance", "bitget"]


def initialize_caches() -> None:
    """Initialize caches with initial data for common exchanges."""
    logger.info("Initializing reference data caches")

    symbol_cache = SymbolInfoCache.get_instance()
    payment_cache = PaymentMethodCache.get_instance()

    # Initialize symbol cache for each exchange
    for exchange in EXCHANGES_TO_CACHE:
        try:
            symbols = symbol_cache.get_all_symbols(exchange)
            logger.info(f"Initialized symbol cache for {exchange} with {len(symbols)} symbols")
        except Exception as e:
            logger.error(f"Error initializing symbol cache for {exchange}: {e}")

    # Initialize payment method cache for exchanges that need it
    for exchange in ["bybit"]:
        try:
            payment_methods = payment_cache.get_all_payment_methods(exchange)
            logger.info(f"Initialized payment method cache for {exchange} with {len(payment_methods)} methods")
        except Exception as e:
            logger.error(f"Error initializing payment method cache for {exchange}: {e}")


def setup_cache_refresh() -> BackgroundScheduler:
    """
    Set up periodic cache refresh.

    Returns:
        Background scheduler
    """
    logger.info("Setting up periodic cache refresh")

    scheduler = BackgroundScheduler()

    # Schedule symbol cache refresh (every 6 hours)
    def refresh_symbol_cache():
        logger.info("Scheduled refresh of symbol cache")
        symbol_cache = SymbolInfoCache.get_instance()

        for exchange in EXCHANGES_TO_CACHE:
            try:
                symbols = symbol_cache.get_all_symbols(exchange)
                logger.info(f"Refreshed symbol cache for {exchange} with {len(symbols)} symbols")
            except Exception as e:
                logger.error(f"Error refreshing symbol cache for {exchange}: {e}")

    scheduler.add_job(
        refresh_symbol_cache,
        'interval',
        hours=6,
        id='symbol_cache_refresh'
    )

    # Schedule payment method cache refresh (every 12 hours)
    def refresh_payment_cache():
        logger.info("Scheduled refresh of payment method cache")
        payment_cache = PaymentMethodCache.get_instance()

        for exchange in ["bybit", "mexc"]:
            try:
                payment_methods = payment_cache.get_all_payment_methods(exchange)
                logger.info(f"Refreshed payment method cache for {exchange} with {len(payment_methods)} methods")
            except Exception as e:
                logger.error(f"Error refreshing payment method cache for {exchange}: {e}")

    scheduler.add_job(
        refresh_payment_cache,
        'interval',
        hours=12,
        id='payment_cache_refresh'
    )

    # Start the scheduler
    scheduler.start()
    logger.info("Cache refresh scheduler started")

    return scheduler