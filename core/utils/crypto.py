from typing import Dict, Any, List, Optional
from datetime import datetime


def calculate_arbitrage_profit(buy_price: float, sell_price: float,
                               amount: float, transfer_fee: float = 0,
                               buy_fee_percent: float = 0,
                               sell_fee_percent: float = 0) -> Dict[str, float]:
    """
    Calculate potential profit from an arbitrage opportunity.

    Args:
        buy_price: Price to buy the asset
        sell_price: Price to sell the asset
        amount: Amount of the asset to trade
        transfer_fee: Fixed fee to transfer between exchanges
        buy_fee_percent: Percentage fee for buying
        sell_fee_percent: Percentage fee for selling

    Returns:
        Dictionary with profit details
    """
    # Calculate costs
    buy_cost = buy_price * amount
    buy_fee = buy_cost * (buy_fee_percent / 100)

    # Calculate revenue
    sell_revenue = sell_price * amount
    sell_fee = sell_revenue * (sell_fee_percent / 100)

    # Calculate net profit
    total_cost = buy_cost + buy_fee + transfer_fee
    total_revenue = sell_revenue - sell_fee
    profit = total_revenue - total_cost
    profit_percent = (profit / total_cost) * 100

    return {
        "buy_cost": buy_cost,
        "buy_fee": buy_fee,
        "sell_revenue": sell_revenue,
        "sell_fee": sell_fee,
        "transfer_fee": transfer_fee,
        "total_cost": total_cost,
        "total_revenue": total_revenue,
        "profit": profit,
        "profit_percent": profit_percent
    }


def get_fiat_name(code: str) -> str:
    """
    Return a human-readable name for the fiat currency code.

    Args:
        code: The currency code (e.g., "USD")

    Returns:
        Human-readable currency name
    """
    fiat_names = {
        "USD": "US Dollar",
        "EUR": "Euro",
        "GBP": "British Pound",
        "JPY": "Japanese Yen",
        "AUD": "Australian Dollar",
        "CAD": "Canadian Dollar",
        "CHF": "Swiss Franc",
        "CNY": "Chinese Yuan",
        "RUB": "Russian Ruble",
        "UZS": "Uzbekistani Som",
        "KZT": "Kazakhstani Tenge",
        "TRY": "Turkish Lira",
        "INR": "Indian Rupee"
    }
    return fiat_names.get(code, code)


def normalize_symbol(symbol: str) -> Dict[str, str]:
    """
    Normalize a trading symbol into base and quote assets.

    Args:
        symbol: Trading symbol (e.g., 'BTCUSDT')

    Returns:
        Dictionary containing 'base' and 'quote' assets
    """
    common_quote_assets = ["USDT", "USDC", "BUSD", "USD", "BTC", "ETH", "BNB", "DAI"]

    for quote in common_quote_assets:
        if symbol.endswith(quote):
            return {
                "base": symbol[:-len(quote)],
                "quote": quote
            }

    # If no match found, make a best guess (last 4 characters as quote)
    return {
        "base": symbol[:-4] if len(symbol) > 4 else symbol,
        "quote": symbol[-4:] if len(symbol) > 4 else ""
    }