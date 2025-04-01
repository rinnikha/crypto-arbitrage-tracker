"""
Mappers for Bitget exchange data.
"""
from core.dto import P2POrderDTO, SpotPairDTO
from core.mapping import Mapper, get_mapper_registry


def create_mexc_mappers():
    """Create and register mappers for MEXC exchange data."""

    mapper_registry = get_mapper_registry()

    p2p_mapper = Mapper(P2POrderDTO)
    p2p_mapper.set_default_value("exchange_name", "MEXC")

    p2p_mapper.map_field("exchange_name", "exchange_name")
    p2p_mapper.map_field("asset_symbol", "coinName")
    p2p_mapper.map_field("fiat_code", "currency")
    p2p_mapper.map_field("price", "price")
    p2p_mapper.map_field("available_amount", "availableQuantity")
    p2p_mapper.map_field("payment_methods", "_payments")
    p2p_mapper.map_field("min_amount", "minTradeLimit")
    p2p_mapper.map_field("max_amount", "maxTradeLimit")
    p2p_mapper.map_field("order_id", "id")
    p2p_mapper.map_field("user_id", "merchant.memberId")
    p2p_mapper.map_field("user_name", "merchant.nickName")
    p2p_mapper.map_field_with_converter("completion_rate", "merchantStatistics.completeRate", lambda x: float(x) * 100)

    p2p_mapper.map_field_with_converter("order_type", "tradeType", lambda x: "BUY" if x == 0 else "SELL")

    # Register the P2P mapper
    mapper_registry.register("mexc_p2p_order", p2p_mapper)

    # Spot pair mapper
    spot_mapper = Mapper(SpotPairDTO)
    spot_mapper.set_default_value("exchange_name", "MEXC")

    spot_mapper.map_field("exchange_name", "exchange_name")
    spot_mapper.map_field("symbol", "symbol")
    spot_mapper.map_field_with_converter("price", "lastPrice", float)
    spot_mapper.map_field_with_converter("bid_price", "bidPrice", float)
    spot_mapper.map_field_with_converter("ask_price", "askPrice", float)
    spot_mapper.map_field_with_converter("volume_24h", "volume", float)
    spot_mapper.map_field_with_converter("high_24h", "highPrice", float)
    spot_mapper.map_field_with_converter("low_24h", "lowPrice", float)
    spot_mapper.map_field("base_asset_symbol", "baseAsset")
    spot_mapper.map_field("quote_asset_symbol", "quoteAsset")

    # Register the spot mapper
    mapper_registry.register("mexc_spot_pair", spot_mapper)