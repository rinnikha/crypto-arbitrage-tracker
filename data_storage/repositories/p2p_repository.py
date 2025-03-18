"""
Repository for P2P market data with optimized batch operations.
"""
import logging
import time
import json
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime
import threading

from sqlalchemy.orm import Session

from core.models import Exchange, Asset, Fiat, P2PSnapshot, P2POrder
from core.dto import P2POrderDTO
from config.exchanges import EXCHANGE_SETTINGS
from data_storage.repositories import BaseRepository
from core.utils.cache import ASSET_CACHE, EXCHANGE_CACHE, FIAT_CACHE

logger = logging.getLogger(__name__)


def get_fiat_name(code: str) -> str:
    """Return a human-readable name for the fiat currency code."""
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
        "TRY": "Turkish Lira",
        "INR": "Indian Rupee",
        "BRL": "Brazilian Real",
        "UAH": "Ukrainian Hryvnia",
        "NGN": "Nigerian Naira",
        "VND": "Vietnamese Dong",
        "IDR": "Indonesian Rupiah",
        "MYR": "Malaysian Ringgit",
        "KZT": "Kazakhstani Tenge",
        "THB": "Thai Baht"
    }
    return fiat_names.get(code, code)


class P2PRepository(BaseRepository[P2POrder, P2POrderDTO]):
    """
    Repository for managing P2P market data with optimized operations.
    """

    def __init__(self, db_session: Session):
        """
        Initialize the repository.

        Args:
            db_session: SQLAlchemy database session
        """
        super().__init__(db_session, P2POrder)

    def create_snapshot(self, timestamp: Optional[datetime] = None) -> P2PSnapshot:
        """
        Create a new P2P snapshot.

        Args:
            timestamp: Timestamp for the snapshot (optional)

        Returns:
            Created P2PSnapshot instance
        """
        if timestamp is None:
            timestamp = datetime.now()

        # Create snapshot with transaction
        with self.transaction():
            snapshot = P2PSnapshot(timestamp=timestamp)
            self.db.add(snapshot)
            self.db.flush()

            logger.info(f"Created P2P snapshot with ID {snapshot.id} at {timestamp}")
            return snapshot

    def add_order(self, snapshot: P2PSnapshot, order_data: P2POrderDTO) -> P2POrder:
        """
        Add a P2P order to a snapshot.

        Args:
            snapshot: Snapshot to add the order to
            order_data: P2P order data transfer object

        Returns:
            Created P2POrder instance
        """
        with self.transaction():
            # Get or create Exchange
            exchange = self._get_or_create_exchange(order_data.exchange_name)

            # Get or create Asset
            asset = self._get_or_create_asset(order_data.asset_symbol)

            # Get or create Fiat
            fiat_code = order_data.fiat_code or "USD"  # Default to USD if not provided
            fiat = self._get_or_create_fiat(fiat_code)

            # Create P2POrder
            order = P2POrder(
                exchange_id=exchange.id,
                asset_id=asset.id,
                fiat_id=fiat.id,
                snapshot_id=snapshot.id,
                price=order_data.price,
                order_type=order_data.order_type,
                available_amount=order_data.available_amount,
                min_amount=order_data.min_amount,
                max_amount=order_data.max_amount,
                payment_methods=order_data.payment_methods,
                order_id=order_data.order_id or f"unknown-{int(time.time())}",
                user_id=order_data.user_id,
                user_name=order_data.user_name,
                completion_rate=order_data.completion_rate
            )

            self.db.add(order)
            self.db.flush()

            return order

    def add_orders_batch(self, snapshot: P2PSnapshot, orders: List[P2POrderDTO]) -> int:
        """
        Add multiple P2P orders to a snapshot in an optimized batch operation.

        Args:
            snapshot: Snapshot to add the orders to
            orders: List of P2P order data transfer objects

        Returns:
            Number of orders added
        """
        if not orders:
            return 0

        start_time = time.time()
        logger.info(f"Starting batch insert of {len(orders)} P2P orders")

        with self.transaction():
            # Prepare reference entities first
            exchange_map = self._prepare_exchanges([order.exchange_name for order in orders])
            asset_map = self._prepare_assets([order.asset_symbol for order in orders])
            fiat_map = self._prepare_fiats([order.fiat_code or "USD" for order in orders])

            # Prepare rows for batch insert
            rows = []
            for order in orders:
                # Skip if missing required references
                exchange_name = order.exchange_name
                asset_symbol = order.asset_symbol
                fiat_code = order.fiat_code or "USD"

                if exchange_name not in exchange_map:
                    logger.warning(f"Missing exchange {exchange_name} for order, skipping")
                    continue
                if asset_symbol not in asset_map:
                    logger.warning(f"Missing asset {asset_symbol} for order, skipping")
                    continue
                if fiat_code not in fiat_map:
                    logger.warning(f"Missing fiat {fiat_code} for order, skipping")
                    continue

                rows.append((
                    exchange_map[exchange_name].id,
                    asset_map[asset_symbol].id,
                    fiat_map[fiat_code].id,
                    snapshot.id,
                    float(order.price or 0),
                    order.order_type,
                    float(order.available_amount or 0),
                    float(order.min_amount or 0),
                    float(order.max_amount or 0),
                    json.dumps(order.payment_methods or []),
                    order.order_id or f"unknown-{int(time.time())}",
                    order.user_id,
                    order.user_name,
                    float(order.completion_rate or 0),
                    datetime.now()
                ))

            # Execute batch insert
            if rows:
                columns = [
                    "exchange_id", "asset_id", "fiat_id", "snapshot_id",
                    "price", "order_type", "available_amount", "min_amount", "max_amount",
                    "payment_methods", "order_id", "user_id", "user_name",
                    "completion_rate", "created_at"
                ]

                inserted = self.batch_insert(rows, columns, "p2p_orders")
                logger.info(f"Inserted {inserted} P2P orders in batch")

                duration = time.time() - start_time
                logger.info(f"Batch insert completed in {duration:.2f} seconds")

                return inserted
            else:
                logger.warning("No valid orders to insert")
                return 0

    def get_latest_snapshot(self) -> Optional[P2PSnapshot]:
        """
        Get the latest P2P snapshot.

        Returns:
            Latest P2PSnapshot instance or None
        """
        return self.db.query(P2PSnapshot).order_by(P2PSnapshot.timestamp.desc()).first()

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[P2PSnapshot]:
        """
        Get a P2P snapshot by ID.

        Args:
            snapshot_id: Snapshot ID

        Returns:
            P2PSnapshot instance or None
        """
        return self.db.query(P2PSnapshot).get(snapshot_id)

    def get_orders_by_snapshot(self, snapshot_id: int,
                               exchange_name: Optional[str] = None,
                               asset_symbol: Optional[str] = None,
                               order_type: Optional[str] = None) -> List[P2POrder]:
        """
        Get orders for a specific snapshot with optional filters.

        Args:
            snapshot_id: Snapshot ID
            exchange_name: Filter by exchange name
            asset_symbol: Filter by asset symbol
            order_type: Filter by order type (BUY, SELL)

        Returns:
            List of P2POrder instances
        """
        query = self.db.query(P2POrder).filter(P2POrder.snapshot_id == snapshot_id)

        if exchange_name:
            query = query.join(Exchange).filter(Exchange.name == exchange_name)

        if asset_symbol:
            query = query.join(Asset).filter(Asset.symbol == asset_symbol)

        if order_type:
            query = query.filter(P2POrder.order_type == order_type)

        return query.all()

    def get_order_by_external_id(self, order_id: str, exchange_name: str) -> Optional[P2POrder]:
        """
        Get a P2P order by its external ID from a specific exchange.

        Args:
            order_id: External order ID
            exchange_name: Exchange name

        Returns:
            P2POrder instance or None
        """
        return self.db.query(P2POrder).join(Exchange).filter(
            P2POrder.order_id == order_id,
            Exchange.name == exchange_name
        ).order_by(P2POrder.created_at.desc()).first()

    def _get_or_create_exchange(self, exchange_name: str) -> Exchange:
        """
        Get or create an Exchange entity.

        Args:
            exchange_name: Exchange name

        Returns:
            Exchange entity
        """
        # Check cache first
        exchange = EXCHANGE_CACHE.get(exchange_name)
        if exchange:
            return exchange

        # Query database
        exchange = self.db.query(Exchange).filter_by(name=exchange_name).first()

        if not exchange:
            # Find exchange settings
            exchange_settings = next(
                (s for k, s in EXCHANGE_SETTINGS.items()
                 if k.lower() == exchange_name.lower() or
                 s['base_url'].find(exchange_name.lower()) != -1),
                {}
            )

            # Create new exchange
            exchange = Exchange(
                name=exchange_name,
                base_url=exchange_settings.get('base_url', ''),
                p2p_url=exchange_settings.get('p2p_url', ''),
                fiat_currencies=exchange_settings.get('fiat_currencies', ["USD"])
            )

            self.db.add(exchange)
            self.db.flush()

        # Cache the result
        EXCHANGE_CACHE.set(exchange_name, exchange)

        return exchange

    def _get_or_create_asset(self, symbol: str) -> Asset:
        """
        Get or create an Asset entity.

        Args:
            symbol: Asset symbol

        Returns:
            Asset entity
        """
        # Check cache first
        asset = ASSET_CACHE.get(symbol)
        if asset:
            return asset

        # Query database
        asset = self.db.query(Asset).filter_by(symbol=symbol).first()

        if not asset:
            # Create new asset
            asset = Asset(
                symbol=symbol,
                name=symbol  # Use symbol as name
            )

            self.db.add(asset)
            self.db.flush()

        # Cache the result
        ASSET_CACHE.set(symbol, asset)

        return asset

    def _get_or_create_fiat(self, code: str) -> Fiat:
        """
        Get or create a Fiat entity.

        Args:
            code: Fiat currency code

        Returns:
            Fiat entity
        """
        # Check cache first
        fiat = FIAT_CACHE.get(code)
        if fiat:
            return fiat

        # Query database
        fiat = self.db.query(Fiat).filter_by(code=code).first()

        if not fiat:
            # Create new fiat
            fiat = Fiat(
                code=code,
                name=get_fiat_name(code)
            )

            self.db.add(fiat)
            self.db.flush()

        # Cache the result
        FIAT_CACHE.set(code, fiat)

        return fiat

    def _prepare_exchanges(self, exchange_names: List[str]) -> Dict[str, Exchange]:
        """
        Prepare Exchange entities for batch operations.

        Args:
            exchange_names: List of exchange names

        Returns:
            Dictionary mapping exchange names to Exchange entities
        """
        # Get unique exchange names
        unique_names = set(exchange_names)

        # Initialize result dictionary
        result = {}

        # Check cache first
        for name in list(unique_names):
            exchange = EXCHANGE_CACHE.get(name)
            if exchange:
                result[name] = exchange
                unique_names.remove(name)

        # If all found in cache, return early
        if not unique_names:
            return result

        # Query existing exchanges
        existing = self.db.query(Exchange).filter(Exchange.name.in_(unique_names)).all()
        for exchange in existing:
            result[exchange.name] = exchange
            EXCHANGE_CACHE.set(exchange.name, exchange)
            unique_names.remove(exchange.name)

        # Create any missing exchanges
        if unique_names:
            exchanges_to_create = []

            for name in unique_names:
                # Find exchange settings
                exchange_settings = next(
                    (s for k, s in EXCHANGE_SETTINGS.items()
                     if k.lower() == name.lower() or
                     s['base_url'].find(name.lower()) != -1),
                    {}
                )

                # Create new exchange
                exchange = Exchange(
                    name=name,
                    base_url=exchange_settings.get('base_url', ''),
                    p2p_url=exchange_settings.get('p2p_url', ''),
                    fiat_currencies=exchange_settings.get('fiat_currencies', ["USD"])
                )

                exchanges_to_create.append(exchange)
                result[name] = exchange

            # Batch add all new exchanges
            self.db.add_all(exchanges_to_create)
            self.db.flush()

            # Cache new exchanges
            for exchange in exchanges_to_create:
                EXCHANGE_CACHE.set(exchange.name, exchange)

        return result

    def _prepare_assets(self, symbols: List[str]) -> Dict[str, Asset]:
        """
        Prepare Asset entities for batch operations.

        Args:
            symbols: List of asset symbols

        Returns:
            Dictionary mapping asset symbols to Asset entities
        """
        # Get unique symbols
        unique_symbols = set(symbols)

        # Initialize result dictionary
        result = {}

        # Check cache first
        for symbol in list(unique_symbols):
            asset = ASSET_CACHE.get(symbol)
            if asset:
                result[symbol] = asset
                unique_symbols.remove(symbol)

        # If all found in cache, return early
        if not unique_symbols:
            return result

        # Query existing assets
        existing = self.db.query(Asset).filter(Asset.symbol.in_(unique_symbols)).all()
        for asset in existing:
            result[asset.symbol] = asset
            ASSET_CACHE.set(asset.symbol, asset)
            unique_symbols.remove(asset.symbol)

        # Create any missing assets
        if unique_symbols:
            assets_to_create = []

            for symbol in unique_symbols:
                asset = Asset(
                    symbol=symbol,
                    name=symbol  # Use symbol as name
                )

                assets_to_create.append(asset)
                result[symbol] = asset

            # Batch add all new assets
            self.db.add_all(assets_to_create)
            self.db.flush()

            # Cache new assets
            for asset in assets_to_create:
                ASSET_CACHE.set(asset.symbol, asset)

        return result

    def _prepare_fiats(self, codes: List[str]) -> Dict[str, Fiat]:
        """
        Prepare Fiat entities for batch operations.

        Args:
            codes: List of fiat currency codes

        Returns:
            Dictionary mapping fiat codes to Fiat entities
        """
        # Get unique codes
        unique_codes = set(codes)

        # Initialize result dictionary
        result = {}

        # Check cache first
        for code in list(unique_codes):
            fiat = FIAT_CACHE.get(code)
            if fiat:
                result[code] = fiat
                unique_codes.remove(code)

        # If all found in cache, return early
        if not unique_codes:
            return result

        # Query existing fiats
        existing = self.db.query(Fiat).filter(Fiat.code.in_(unique_codes)).all()
        for fiat in existing:
            result[fiat.code] = fiat
            FIAT_CACHE.set(fiat.code, fiat)
            unique_codes.remove(fiat.code)

        # Create any missing fiats
        if unique_codes:
            fiats_to_create = []

            for code in unique_codes:
                fiat = Fiat(
                    code=code,
                    name=get_fiat_name(code)
                )

                fiats_to_create.append(fiat)
                result[code] = fiat

            # Batch add all new fiats
            self.db.add_all(fiats_to_create)
            self.db.flush()

            # Cache new fiats
            for fiat in fiats_to_create:
                FIAT_CACHE.set(fiat.code, fiat)

        return result

    def _dto_to_entity(self, dto: P2POrderDTO) -> P2POrder:
        """
        Convert a DTO to an entity.

        Args:
            dto: DTO to convert

        Returns:
            Entity
        """
        # Get required references
        exchange = self._get_or_create_exchange(dto.exchange_name)
        asset = self._get_or_create_asset(dto.asset_symbol)
        fiat = self._get_or_create_fiat(dto.fiat_code or "USD")

        # Create entity
        return P2POrder(
            exchange_id=exchange.id,
            asset_id=asset.id,
            fiat_id=fiat.id,
            price=dto.price,
            order_type=dto.order_type,
            available_amount=dto.available_amount,
            min_amount=dto.min_amount,
            max_amount=dto.max_amount,
            payment_methods=dto.payment_methods,
            order_id=dto.order_id or f"unknown-{int(time.time())}",
            user_id=dto.user_id,
            user_name=dto.user_name,
            completion_rate=dto.completion_rate
        )