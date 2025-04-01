"""
Mappers for Bitget exchange data.
"""
from core.dto import P2POrderDTO, SpotPairDTO
from core.mapping import Mapper, get_mapper_registry


def create_bitget_mappers():
    """Create and register mappers for Bitget exchange data."""
    mapper_registry = get_mapper_registry()

    # P2P order mapper
    p2p_mapper = Mapper(P2POrderDTO)
    p2p_mapper.set_default_value("exchange_name", "Bitget")
    p2p_mapper.set_default_value("fiat_code", "USD")

    p2p_mapper.map_field("exchange_name", "exchange_name")
    p2p_mapper.map_field("fiat_code", "fiat_code")

    # Map fields with custom functions for complex transformations
    p2p_mapper.map_field_with_converter(
        "asset_symbol",
        "coin",
        lambda x: x.upper() if x else ""
    )
    p2p_mapper.map_field_with_converter(
        "price",
        "price",
        lambda x: float(x) if x else 0
    )
    p2p_mapper.map_field_with_converter(
        "order_type",
        "side",
        lambda x: x.upper()
    )
    p2p_mapper.map_field_with_converter(
        "available_amount",
        "advSize",
        lambda x: float(x) if x else 0
    )
    p2p_mapper.map_field_with_converter(
        "min_amount",
        "minTradeAmount",
        lambda x: float(x) if x else 0
    )
    p2p_mapper.map_field_with_converter(
        "max_amount",
        "maxTradeAmount",
        lambda x: float(x) if x else 0
    )
    p2p_mapper.map_field_with_converter(
        "payment_methods",
        "paymentMethodList",
        lambda x: [pm.get('paymentMethod', '') for pm in x] if x else []
    )
    p2p_mapper.map_field("order_id", "advNo")
    p2p_mapper.map_field("user_id", "userId")
    p2p_mapper.map_field("user_name", "userName")
    p2p_mapper.map_field_with_converter(
        "completion_rate",
        "turnoverRate",
        lambda x: float(x) * 100 if x else 0
    )

    # Register the P2P mapper
    mapper_registry.register("bitget_p2p_order", p2p_mapper)

    # Spot pair mapper
    spot_mapper = Mapper(SpotPairDTO)
    spot_mapper.set_default_value("exchange_name", "Bitget")

    spot_mapper.map_field("exchange_name", "exchange_name")
    spot_mapper.map_field("symbol", "symbol")
    spot_mapper.map_field("price", "lastPr")
    spot_mapper.map_field("bid_price", "bidPr")
    spot_mapper.map_field("ask_price", "askPr")
    spot_mapper.map_field("volume_24h", "baseVolume")
    spot_mapper.map_field("high_24h", "high24h")
    spot_mapper.map_field("low_24h", "low24h")
    spot_mapper.map_field("base_asset_symbol", "baseAsset")
    spot_mapper.map_field("quote_asset_symbol", "quoteAsset")

    # Register the spot mapper
    mapper_registry.register("bitget_spot_pair", spot_mapper)