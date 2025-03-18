"""
Package for exchange-specific mappers.
"""
# Import all mappers to ensure they're registered
from data_collection.api_clients.mappers.binance_mappers import create_binance_mappers

# Initialize all mappers
def initialize_mappers():
    """Initialize all exchange mappers."""
    create_binance_mappers()
    # Add initialization for other exchange mappers here
    # create_bybit_mappers()
    # create_bitget_mappers()
    # etc.

# Auto-initialize mappers when the package is imported
initialize_mappers()