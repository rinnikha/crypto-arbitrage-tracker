"""
Exchange-specific configuration settings.
"""
import os

# Binance Settings
BINANCE_ENABLED = os.getenv("BINANCE_ENABLED", "True").lower() in ("true", "yes", "1", "y")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
BINANCE_BASE_URL = "https://api.binance.com"
BINANCE_P2P_URL = "https://p2p.binance.com/bapi/c2c/v2/friendly/c2c/adv/search"
BINANCE_SPOT_BATCH_SIZE = 100
BINANCE_P2P_BATCH_SIZE = 20
BINANCE_FIAT_CURRENCIES = ["USD", "EUR", "GBP"]
BINANCE_DEFAULT_FIAT = "USD"

# Bitget Settings
BITGET_ENABLED = os.getenv("BITGET_ENABLED", "True").lower() in ("true", "yes", "1", "y")
BITGET_API_KEY = os.getenv("BITGET_API_KEY", "")
BITGET_API_SECRET = os.getenv("BITGET_API_SECRET", "")
BITGET_PASSPHRASE = os.getenv("BITGET_PASSPHRASE", "")
BITGET_BASE_URL = "https://api.bitget.com"
BITGET_P2P_URL = "https://api.bitget.com/api/spot/v1/p2p/merchant/advertise/list"
BITGET_SPOT_BATCH_SIZE = 100
BITGET_P2P_BATCH_SIZE = 20
BITGET_FIAT_CURRENCIES = ["USD", "EUR"]
BITGET_DEFAULT_FIAT = "USD"

# Bybit Settings
BYBIT_ENABLED = os.getenv("BYBIT_ENABLED", "True").lower() in ("true", "yes", "1", "y")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
BYBIT_BASE_URL = "https://api.bybit.com"
BYBIT_P2P_URL = "https://api.bybit.com/v5/spot/c2c/order-book"
BYBIT_SPOT_BATCH_SIZE = 50
BYBIT_P2P_BATCH_SIZE = 20
BYBIT_FIAT_CURRENCIES = ["USD", "EUR"]
BYBIT_DEFAULT_FIAT = "USD"

# MEXC Settings
MEXC_ENABLED = os.getenv("MEXC_ENABLED", "True").lower() in ("true", "yes", "1", "y")
MEXC_API_KEY = os.getenv("MEXC_API_KEY", "")
MEXC_API_SECRET = os.getenv("MEXC_API_SECRET", "")
MEXC_BASE_URL = "https://api.mexc.com"
MEXC_P2P_URL = "https://otc.mexc.com/api"
MEXC_SPOT_BATCH_SIZE = 50
MEXC_P2P_BATCH_SIZE = 20
MEXC_FIAT_CURRENCIES = ["USD"]
MEXC_DEFAULT_FIAT = "USD"

# TON Wallet Settings
TON_ENABLED = os.getenv("TON_ENABLED", "True").lower() in ("true", "yes", "1", "y")
TON_API_TOKEN = os.getenv("TON_API_TOKEN", "")
TON_BASE_URL = "https://toncenter.com/api/v2"
TON_SPOT_BATCH_SIZE = 10
TON_P2P_BATCH_SIZE = 10
TON_FIAT_CURRENCIES = ["USD"]
TON_DEFAULT_FIAT = "USD"

# Transfer fees
BINANCE_BITGET_FEE = float(os.getenv("BINANCE_BITGET_FEE", "1.0"))
BINANCE_BITGET_PERCENT = float(os.getenv("BINANCE_BITGET_PERCENT", "0.0"))
BINANCE_BYBIT_FEE = float(os.getenv("BINANCE_BYBIT_FEE", "1.0"))
BINANCE_BYBIT_PERCENT = float(os.getenv("BINANCE_BYBIT_PERCENT", "0.0"))
BITGET_BINANCE_FEE = float(os.getenv("BITGET_BINANCE_FEE", "1.0"))
BITGET_BINANCE_PERCENT = float(os.getenv("BITGET_BINANCE_PERCENT", "0.0"))
BYBIT_BINANCE_FEE = float(os.getenv("BYBIT_BINANCE_FEE", "1.0"))
BYBIT_BINANCE_PERCENT = float(os.getenv("BYBIT_BINANCE_PERCENT", "0.0"))
MEXC_BINANCE_FEE = float(os.getenv("MEXC_BINANCE_FEE", "1.0"))
MEXC_BINANCE_PERCENT = float(os.getenv("MEXC_BINANCE_PERCENT", "0.0"))

# Trading fees
BINANCE_MAKER_FEE = float(os.getenv("BINANCE_MAKER_FEE", "0.1"))
BINANCE_TAKER_FEE = float(os.getenv("BINANCE_TAKER_FEE", "0.1"))
BITGET_MAKER_FEE = float(os.getenv("BITGET_MAKER_FEE", "0.1"))
BITGET_TAKER_FEE = float(os.getenv("BITGET_TAKER_FEE", "0.1"))
BYBIT_MAKER_FEE = float(os.getenv("BYBIT_MAKER_FEE", "0.1"))
BYBIT_TAKER_FEE = float(os.getenv("BYBIT_TAKER_FEE", "0.1"))
MEXC_MAKER_FEE = float(os.getenv("MEXC_MAKER_FEE", "0.1"))
MEXC_TAKER_FEE = float(os.getenv("MEXC_TAKER_FEE", "0.1"))

# Recreate the dictionaries for compatibility
EXCHANGE_SETTINGS = {
    "binance": {
        "enabled": BINANCE_ENABLED,
        "api_key": BINANCE_API_KEY,
        "api_secret": BINANCE_API_SECRET,
        "base_url": BINANCE_BASE_URL,
        "p2p_url": BINANCE_P2P_URL,
        "spot_batch_size": BINANCE_SPOT_BATCH_SIZE,
        "p2p_batch_size": BINANCE_P2P_BATCH_SIZE,
        "fiat_currencies": BINANCE_FIAT_CURRENCIES,
        "default_fiat": BINANCE_DEFAULT_FIAT
    },
    "bitget": {
        "enabled": BITGET_ENABLED,
        "api_key": BITGET_API_KEY,
        "api_secret": BITGET_API_SECRET,
        "passphrase": BITGET_PASSPHRASE,
        "base_url": BITGET_BASE_URL,
        "p2p_url": BITGET_P2P_URL,
        "spot_batch_size": BITGET_SPOT_BATCH_SIZE,
        "p2p_batch_size": BITGET_P2P_BATCH_SIZE,
        "fiat_currencies": BITGET_FIAT_CURRENCIES,
        "default_fiat": BITGET_DEFAULT_FIAT
    },
    "bybit": {
        "enabled": BYBIT_ENABLED,
        "api_key": BYBIT_API_KEY,
        "api_secret": BYBIT_API_SECRET,
        "base_url": BYBIT_BASE_URL,
        "p2p_url": BYBIT_P2P_URL,
        "spot_batch_size": BYBIT_SPOT_BATCH_SIZE,
        "p2p_batch_size": BYBIT_P2P_BATCH_SIZE,
        "fiat_currencies": BYBIT_FIAT_CURRENCIES,
        "default_fiat": BYBIT_DEFAULT_FIAT
    },
    "mexc": {
        "enabled": MEXC_ENABLED,
        "api_key": MEXC_API_KEY,
        "api_secret": MEXC_API_SECRET,
        "base_url": MEXC_BASE_URL,
        "p2p_url": MEXC_P2P_URL,
        "spot_batch_size": MEXC_SPOT_BATCH_SIZE,
        "p2p_batch_size": MEXC_P2P_BATCH_SIZE,
        "fiat_currencies": MEXC_FIAT_CURRENCIES,
        "default_fiat": MEXC_DEFAULT_FIAT
    },
    "ton_wallet": {
        "enabled": TON_ENABLED,
        "api_token": TON_API_TOKEN,
        "base_url": TON_BASE_URL,
        "spot_batch_size": TON_SPOT_BATCH_SIZE,
        "p2p_batch_size": TON_P2P_BATCH_SIZE,
        "fiat_currencies": TON_FIAT_CURRENCIES,
        "default_fiat": TON_DEFAULT_FIAT
    }
}

TRANSFER_METHODS = {
    "binance_to_bitget": {
        "network": "TRC20",
        "fixed_fee": BINANCE_BITGET_FEE,
        "percentage_fee": BINANCE_BITGET_PERCENT,
        "estimated_time_minutes": 15
    },
    "binance_to_bybit": {
        "network": "TRC20",
        "fixed_fee": BINANCE_BYBIT_FEE,
        "percentage_fee": BINANCE_BYBIT_PERCENT,
        "estimated_time_minutes": 15
    },
    "bitget_to_binance": {
        "network": "TRC20",
        "fixed_fee": BITGET_BINANCE_FEE,
        "percentage_fee": BITGET_BINANCE_PERCENT,
        "estimated_time_minutes": 15
    },
    "bybit_to_binance": {
        "network": "TRC20",
        "fixed_fee": BYBIT_BINANCE_FEE,
        "percentage_fee": BYBIT_BINANCE_PERCENT,
        "estimated_time_minutes": 15
    },
    "mexc_to_binance": {
        "network": "TRC20",
        "fixed_fee": MEXC_BINANCE_FEE,
        "percentage_fee": MEXC_BINANCE_PERCENT,
        "estimated_time_minutes": 20
    }
}

TRADING_FEES = {
    "binance": {
        "maker": BINANCE_MAKER_FEE,
        "taker": BINANCE_TAKER_FEE
    },
    "bitget": {
        "maker": BITGET_MAKER_FEE,
        "taker": BITGET_TAKER_FEE
    },
    "bybit": {
        "maker": BYBIT_MAKER_FEE,
        "taker": BYBIT_TAKER_FEE
    },
    "mexc": {
        "maker": MEXC_MAKER_FEE,
        "taker": MEXC_TAKER_FEE
    }
}

ALTERNATIVE_ENDPOINTS = {
    "binance": {
        "ticker": "https://www.binance.com/api/v3/ticker/24hr",
        "depth": "https://www.binance.com/api/v3/depth"
    },
    "bitget": {
        "ticker": "https://api.bitget.com/api/spot/v1/market/tickers"
    }
}