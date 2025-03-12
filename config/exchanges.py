# config/exchanges.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Exchange API keys and settings
EXCHANGE_SETTINGS = {
    "binance": {
        "enabled": os.getenv("BINANCE_ENABLED", "True").lower() == "true",
        "api_key": os.getenv("BINANCE_API_KEY", ""),
        "api_secret": os.getenv("BINANCE_API_SECRET", ""),
        "base_url": "https://api.binance.com",
        "p2p_url": "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search",
        "fiat_currencies": ["USD", "EUR", "GBP"]
    },
    "bitget": {
        "enabled": os.getenv("BITGET_ENABLED", "True").lower() == "true",
        "api_key": os.getenv("BITGET_API_KEY", ""),
        "api_secret": os.getenv("BITGET_API_SECRET", ""),
        "passphrase": os.getenv("BITGET_PASSPHRASE", ""),
        "base_url": "https://api.bitget.com",
        "p2p_url": "https://api.bitget.com/api/spot/v1/p2p/merchant/advertise/list",
        "fiat_currencies": ["USD", "EUR"]
    },
    "bybit": {
        "enabled": os.getenv("BYBIT_ENABLED", "True").lower() == "true",
        "api_key": os.getenv("BYBIT_API_KEY", ""),
        "api_secret": os.getenv("BYBIT_API_SECRET", ""),
        "base_url": "https://api.bybit.com",
        "p2p_url": "https://api.bybit.com/v5/spot/c2c/order-book",
        "fiat_currencies": ["USD", "EUR"]
    },
    "mexc": {
        "enabled": os.getenv("MEXC_ENABLED", "True").lower() == "true",
        "api_key": os.getenv("MEXC_API_KEY", ""),
        "api_secret": os.getenv("MEXC_API_SECRET", ""),
        "base_url": "https://api.mexc.com",
        "p2p_url": "https://otc.mexc.com/api",
        "fiat_currencies": ["USD"]
    },
    "ton_wallet": {
        "enabled": os.getenv("TON_ENABLED", "True").lower() == "true",
        "api_token": os.getenv("TON_API_TOKEN", ""),
        "base_url": "https://toncenter.com/api/v2",
        "fiat_currencies": ["USD"]
    }
}

# Track these assets across exchanges
ASSETS = ["USDT", "BTC", "ETH", "TON"]

# Define transfer methods and associated fees
TRANSFER_METHODS = {
    "binance_to_bitget": {
        "network": "TRC20",
        "fixed_fee": 1.0,
        "percentage_fee": 0.0
    },
    "binance_to_bybit": {
        "network": "TRC20",
        "fixed_fee": 1.0,
        "percentage_fee": 0.0
    },
    "bitget_to_binance": {
        "network": "TRC20",
        "fixed_fee": 1.0,
        "percentage_fee": 0.0
    }
    # Add more transfer methods as needed
}