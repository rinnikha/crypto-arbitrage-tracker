# core/utils.py
import json
import time
import logging
import hmac
import hashlib
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import requests
from functools import wraps

from config.settings import MAX_RETRIES, REQUEST_TIMEOUT, USER_AGENT

# Set up logging
logger = logging.getLogger(__name__)

def retry_on_failure(max_retries=3, backoff_factor=0.5, exceptions=(requests.RequestException,)):
    """Decorator for retrying a function with exponential backoff on exceptions."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries <= max_retries:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    wait_time = backoff_factor * (2 ** retries)
                    logger.warning(
                        f"Retry {retries}/{max_retries} for {func.__name__} after {wait_time}s due to {e}"
                    )
                    retries += 1
                    if retries > max_retries:
                        raise
                    time.sleep(wait_time)
        return wrapper
    return decorator

@retry_on_failure(max_retries=MAX_RETRIES)
def make_request(url: str, method: str = "GET", params: Optional[Dict] = None, 
                 data: Optional[Dict] = None, headers: Optional[Dict] = None,
                 timeout: int = REQUEST_TIMEOUT) -> requests.Response:
    """
    Make an HTTP request with retry logic.
    
    Args:
        url: The URL to request
        method: HTTP method (GET, POST, etc.)
        params: URL parameters
        data: Request body
        headers: HTTP headers
        timeout: Request timeout in seconds
        
    Returns:
        Response object
    """
    default_headers = {
        "User-Agent": USER_AGENT
    }
    
    if headers:
        default_headers.update(headers)
    
    response = requests.request(
        method=method,
        url=url,
        params=params,
        json=data if method.upper() != "GET" else None,
        headers=default_headers,
        timeout=timeout
    )
    response.raise_for_status()
    return response

def format_number(num: float, decimals: int = 2) -> str:
    """Format a number with the specified number of decimal places."""
    return f"{num:.{decimals}f}"

def parse_datetime(dt_str: str) -> datetime:
    """Parse a datetime string in multiple formats."""
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # ISO format with milliseconds
        "%Y-%m-%dT%H:%M:%SZ",      # ISO format without milliseconds
        "%Y-%m-%d %H:%M:%S",       # Standard format
        "%Y-%m-%d"                 # Date only
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(dt_str, fmt)
        except ValueError:
            continue
    
    raise ValueError(f"Time data '{dt_str}' does not match any known format")

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