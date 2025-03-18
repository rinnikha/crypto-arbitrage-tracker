"""
Application-wide constants.

This file contains constants that are used across the application
and are unlikely to change frequently.
"""
from enum import Enum, auto
from typing import Dict, List


# Order types
class OrderType(str, Enum):
    """P2P order types."""
    BUY = "BUY"  # User buys crypto
    SELL = "SELL"  # User sells crypto


# Fiat currency information
FIAT_CURRENCIES = {
    "USD": {
        "name": "US Dollar",
        "symbol": "$",
        "decimals": 2
    },
    "EUR": {
        "name": "Euro",
        "symbol": "€",
        "decimals": 2
    },
    "GBP": {
        "name": "British Pound",
        "symbol": "£",
        "decimals": 2
    },
    "JPY": {
        "name": "Japanese Yen",
        "symbol": "¥",
        "decimals": 0
    },
    "AUD": {
        "name": "Australian Dollar",
        "symbol": "A$",
        "decimals": 2
    },
    "CAD": {
        "name": "Canadian Dollar",
        "symbol": "C$",
        "decimals": 2
    },
    "CHF": {
        "name": "Swiss Franc",
        "symbol": "Fr",
        "decimals": 2
    },
    "CNY": {
        "name": "Chinese Yuan",
        "symbol": "¥",
        "decimals": 2
    },
    "RUB": {
        "name": "Russian Ruble",
        "symbol": "₽",
        "decimals": 2
    },
    "UZS": {
        "name": "Uzbekistani Som",
        "symbol": "UZS",
        "decimals": 2
    }
}

# Asset information
ASSET_INFO = {
    "BTC": {
        "name": "Bitcoin",
        "decimals": 8,
        "min_transfer": 0.00001
    },
    "ETH": {
        "name": "Ethereum",
        "decimals": 8,
        "min_transfer": 0.0001
    },
    "TON": {
        "name": "Toncoin",
        "decimals": 6,
        "min_transfer": 0.01
    },
    "USDT": {
        "name": "Tether USD",
        "decimals": 6,
        "min_transfer": 0.1
    }
}


# Network types for transfers
class NetworkType(str, Enum):
    """Blockchain network types for transfers."""
    TRC20 = "TRC20"
    ERC20 = "ERC20"
    BEP20 = "BEP20"  # Binance Smart Chain
    BEP2 = "BEP2"  # Binance Chain
    OMNI = "OMNI"
    TON = "TON"


# Common payment methods
PAYMENT_METHODS = {
    "BANK": "Bank Transfer",
    "CARD": "Card Payment",
    "ALIPAY": "Alipay",
    "WECHAT": "WeChat Pay",
    "QIWI": "QIWI",
    "YANDEX": "Yandex Money",
    "PAYPAL": "PayPal",
    "CASH": "Cash",
    "SEPA": "SEPA Transfer",
    "UPI": "UPI",
    "PAYTM": "Paytm",
    "PAYONEER": "Payoneer",
    "SWIFT": "SWIFT Transfer",
    "REVOLUT": "Revolut",
    "WISE": "Wise (TransferWise)",
    "VENMO": "Venmo",
    "ZELLE": "Zelle",
    "CASHAPP": "Cash App"
}

# User input validation
INPUT_VALIDATION_PATTERNS = {
    "asset_symbol": r"^[A-Z0-9]{2,10}$",
    "fiat_code": r"^[A-Z]{3}$",
    "order_type": r"^(BUY|SELL)$",
    "exchange_name": r"^[A-Za-z0-9_]{3,20}$",
    "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
    "username": r"^[a-zA-Z0-9_]{3,30}$",
    "password": r"^.{8,}$"  # At least 8 characters
}


# Application error codes
class ErrorCode(str, Enum):
    """Application error codes."""
    # General errors
    UNKNOWN_ERROR = "UNKNOWN_ERROR"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    NOT_FOUND = "NOT_FOUND"
    UNAUTHORIZED = "UNAUTHORIZED"
    FORBIDDEN = "FORBIDDEN"

    # Database errors
    DATABASE_ERROR = "DATABASE_ERROR"
    DATABASE_CONNECTION_ERROR = "DATABASE_CONNECTION_ERROR"

    # API errors
    API_ERROR = "API_ERROR"
    API_TIMEOUT = "API_TIMEOUT"
    API_RATE_LIMIT = "API_RATE_LIMIT"

    # Exchange-specific errors
    EXCHANGE_CONNECTION_ERROR = "EXCHANGE_CONNECTION_ERROR"
    EXCHANGE_API_ERROR = "EXCHANGE_API_ERROR"
    EXCHANGE_NOT_SUPPORTED = "EXCHANGE_NOT_SUPPORTED"

    # Data errors
    INVALID_DATA = "INVALID_DATA"
    MISSING_DATA = "MISSING_DATA"
    DATA_CONVERSION_ERROR = "DATA_CONVERSION_ERROR"


# HTTP status codes
HTTP_STATUS_CODES = {
    200: "OK",
    201: "Created",
    204: "No Content",
    400: "Bad Request",
    401: "Unauthorized",
    403: "Forbidden",
    404: "Not Found",
    408: "Request Timeout",
    409: "Conflict",
    422: "Unprocessable Entity",
    429: "Too Many Requests",
    500: "Internal Server Error",
    502: "Bad Gateway",
    503: "Service Unavailable",
    504: "Gateway Timeout"
}