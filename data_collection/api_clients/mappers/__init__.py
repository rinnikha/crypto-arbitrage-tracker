"""
Package for exchange-specific mappers.
"""
# Import all mappers to ensure they're registered
from .binance_mappers import create_binance_mappers
from .bitget_mappers import create_bitget_mappers
from .bybit_mappers import create_bybit_mappers
from .mexc_mappers import create_mexc_mappers


# Initialize all mappers
def initialize_mappers():
    """Initialize all exchange mappers."""
    create_binance_mappers()
    create_bitget_mappers()
    create_bybit_mappers()
    create_mexc_mappers()

    # Add initialization for other exchange mappers here
    # create_bybit_mappers()
    # etc.

# Auto-initialize mappers when the package is imported
initialize_mappers()