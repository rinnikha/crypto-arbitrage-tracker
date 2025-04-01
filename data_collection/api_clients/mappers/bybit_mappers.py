"""
Mappers for Bybit exchange data.
"""
import logging
from typing import Dict, Any, Optional
import json
import os

from core.dto import P2POrderDTO, SpotPairDTO
from core.mapping import Mapper, get_mapper_registry

logger = logging.getLogger(__name__)

def load_payments_data(file_path='./bybit_mappers.json'):
    try:
        # Get the directory where the script is located
        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Join the script directory with the file path
        full_path = os.path.join(script_dir, file_path)

        with open(full_path, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        logger.warning(f"Error: File not found at {full_path}")
        return []
    except json.JSONDecodeError:
        logger.warning(f"Error: Invalid JSON format in the file")
        return []

bybit_payments_path = "./bybit_payments.json"

bybit_payments_list = load_payments_data(bybit_payments_path)

bybit_payments_dict = {item["paymentType"]: item["paymentName"] for item in bybit_payments_list}

def create_bybit_mappers():
    """Create and register mappers for Bybit exchange data."""
    mapper_registry = get_mapper_registry()

    # P2P order mapper
    p2p_mapper = Mapper(P2POrderDTO)
    p2p_mapper.set_default_value("exchange_name", "Bybit")

    p2p_mapper.map_field("exchange_name", "exchange_name")
    p2p_mapper.map_field("asset_symbol", "tokenId")
    p2p_mapper.map_field("fiat_code", "currencyId")
    p2p_mapper.map_field("price", "price")
    p2p_mapper.map_field("available_amount", "quantity")
    p2p_mapper.map_field("payment_methods", "_payments")
    p2p_mapper.map_field("min_amount", "minAmount")
    p2p_mapper.map_field("max_amount", "quantity")
    p2p_mapper.map_field("order_id", "id")
    p2p_mapper.map_field("user_id", "userId")
    p2p_mapper.map_field("user_name", "nickName")
    p2p_mapper.map_field("completion_rate", "recentExecuteRate")

    p2p_mapper.map_field_with_converter("order_type", "side", lambda x: "BUY" if x == 0 else "SELL")

    # Register the P2P mapper
    mapper_registry.register("bybit_p2p_order", p2p_mapper)

    # Spot pair mapper
    spot_mapper = Mapper(SpotPairDTO)
    spot_mapper.set_default_value("exchange_name", "Bybit")

    spot_mapper.map_field("exchange_name", "exchange_name")
    spot_mapper.map_field("symbol", "symbol")
    spot_mapper.map_field_with_converter("price", "lastPrice", float)
    spot_mapper.map_field_with_converter("bid_price", "bid1Price", float)
    spot_mapper.map_field_with_converter("ask_price", "ask1Price", float)
    spot_mapper.map_field_with_converter("volume_24h", "volume24h", float)
    spot_mapper.map_field_with_converter("high_24h", "highPrice24h", float)
    spot_mapper.map_field_with_converter("low_24h", "lowPrice24h", float)
    spot_mapper.map_field("base_asset_symbol", "baseAsset")
    spot_mapper.map_field("quote_asset_symbol", "quoteAsset")


    # Custom mapping function for spot pairs
    def map_spot_pair(ticker: Dict[str, Any]) -> Dict[str, Any]:
        symbol = ticker.get('symbol', '')

        # Extract base and quote assets from symbol
        base_asset_symbol = None
        quote_asset_symbol = None

        for quote in ["USDT", "USDC", "BTC", "ETH"]:
            if symbol.endswith(quote):
                quote_asset_symbol = quote
                base_asset_symbol = symbol[:-len(quote)]
                break

        # Handle empty string values in numeric fields
        def safe_float(value):
            if value is None or value == '':
                return 0.0
            try:
                return float(value)
            except (ValueError, TypeError):
                logger.warning(f"Could not convert {value} to float")
                return 0.0

        return {
            "symbol": symbol,
            "price": safe_float(ticker.get('lastPrice')),
            "bid_price": safe_float(ticker.get('bid1Price')),
            "ask_price": safe_float(ticker.get('ask1Price')),
            "volume_24h": safe_float(ticker.get('volume24h')),
            "high_24h": safe_float(ticker.get('highPrice24h')),
            "low_24h": safe_float(ticker.get('lowPrice24h')),
            "base_asset_symbol": base_asset_symbol,
            "quote_asset_symbol": quote_asset_symbol
        }


    # Register the spot mapper
    mapper_registry.register("bybit_spot_pair", spot_mapper)