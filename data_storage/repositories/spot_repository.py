"""
Repository for spot market data with optimized batch operations.
"""
import logging
import time
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime

from sqlalchemy.orm import Session

from core.models import Exchange, Asset, SpotSnapshot, SpotPair
from core.dto import SpotPairDTO
from config.exchanges import EXCHANGE_SETTINGS
from data_storage.repositories.base_repository import BaseRepository
from core.utils.cache import ASSET_CACHE, EXCHANGE_CACHE

logger = logging.getLogger(__name__)


class SpotRepository(BaseRepository[SpotPair, SpotPairDTO]):
    """
    Repository for managing spot market data with optimized operations.
    """

    def __init__(self, db_session: Session):
        """
        Initialize the repository.

        Args:
            db_session: SQLAlchemy database session
        """
        super().__init__(db_session, SpotPair)

    def create_snapshot(self, timestamp: Optional[datetime] = None) -> SpotSnapshot:
        """
        Create a new spot snapshot.

        Args:
            timestamp: Timestamp for the snapshot (optional)

        Returns:
            Created SpotSnapshot instance
        """
        if timestamp is None:
            timestamp = datetime.now()

        # Create snapshot with transaction
        with self.transaction():
            snapshot = SpotSnapshot(timestamp=timestamp)
            self.db.add(snapshot)
            self.db.flush()

            self.db.commit()

            logger.info(f"Created spot snapshot with ID {snapshot.id} at {timestamp}")
            return snapshot

    def add_pair(self, snapshot: SpotSnapshot, pair_data: SpotPairDTO) -> SpotPair:
        """
        Add a spot market pair to a snapshot.

        Args:
            snapshot: Snapshot to add the pair to
            pair_data: Spot pair data transfer object

        Returns:
            Created SpotPair instance
        """
        with self.transaction():
            # Get or create Exchange
            exchange = self._get_or_create_exchange(pair_data.exchange_name)

            # Get or create Base Asset
            base_asset = self._get_or_create_asset(pair_data.base_asset_symbol)

            # Get or create Quote Asset
            quote_asset = self._get_or_create_asset(pair_data.quote_asset_symbol)

            # Create SpotPair
            pair = SpotPair(
                exchange_id=exchange.id,
                base_asset_id=base_asset.id,
                quote_asset_id=quote_asset.id,
                snapshot_id=snapshot.id,
                symbol=pair_data.symbol,
                price=pair_data.price,
                bid_price=pair_data.bid_price,
                ask_price=pair_data.ask_price,
                volume_24h=pair_data.volume_24h,
                high_24h=pair_data.high_24h,
                low_24h=pair_data.low_24h
            )

            self.db.add(pair)
            self.db.flush()

            return pair

    def add_pairs_batch(self, snapshot: SpotSnapshot, pairs: List[SpotPairDTO]) -> int:
        """
        Add multiple spot market pairs to a snapshot in an optimized batch operation.

        Args:
            snapshot: Snapshot to add the pairs to
            pairs: List of spot pair data transfer objects

        Returns:
            Number of pairs added
        """
        if not pairs:
            return 0

        start_time = time.time()
        logger.info(f"Starting batch insert of {len(pairs)} spot pairs")

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                with self.transaction():
                    # Prepare all assets
                    base_asset_symbols = [pair.base_asset_symbol for pair in pairs]
                    quote_asset_symbols = [pair.quote_asset_symbol for pair in pairs]
                    all_symbols = list(set(base_asset_symbols + quote_asset_symbols))

                    asset_map = self._prepare_assets(all_symbols)
                    exchange_map = self._prepare_exchanges([pair.exchange_name for pair in pairs])

                    # Prepare rows for batch insert
                    rows = []
                    for pair in pairs:
                        # Skip if missing required references
                        exchange_name = pair.exchange_name
                        base_symbol = pair.base_asset_symbol
                        quote_symbol = pair.quote_asset_symbol

                        if not base_symbol or not quote_symbol:
                            logger.warning(f"Missing asset symbols for pair {pair.symbol}, skipping")
                            continue

                        if exchange_name not in exchange_map:
                            logger.warning(f"Missing exchange {exchange_name} for pair, skipping")
                            continue
                        if base_symbol not in asset_map:
                            logger.warning(f"Missing base asset {base_symbol} for pair, skipping")
                            continue
                        if quote_symbol not in asset_map:
                            logger.warning(f"Missing quote asset {quote_symbol} for pair, skipping")
                            continue

                        rows.append((
                            exchange_map[exchange_name].id,
                            asset_map[base_symbol].id,
                            asset_map[quote_symbol].id,
                            snapshot.id,
                            pair.symbol,
                            float(pair.price or 0),
                            float(pair.bid_price or 0),
                            float(pair.ask_price or 0),
                            float(pair.volume_24h or 0),
                            float(pair.high_24h or 0),
                            float(pair.low_24h or 0),
                            datetime.now()
                        ))

                    # Execute batch insert
                    if rows:
                        columns = [
                            "exchange_id", "base_asset_id", "quote_asset_id", "snapshot_id",
                            "symbol", "price", "bid_price", "ask_price",
                            "volume_24h", "high_24h", "low_24h", "created_at"
                        ]

                        inserted = self.batch_insert(rows, columns, "spot_pairs")
                        logger.info(f"Inserted {inserted} spot pairs in batch")

                        duration = time.time() - start_time
                        logger.info(f"Batch insert completed in {duration:.2f} seconds")

                        return inserted
                    else:
                        logger.warning("No valid pairs to insert")
                        return 0

            except Exception as e:
                retry_count += 1
                error_msg = str(e)
                logger.warning(f"Attempt {retry_count}/{max_retries} failed: {error_msg}")
                
                if "violates foreign key constraint" in error_msg and retry_count < max_retries:
                    # If it's a foreign key violation, wait a bit before retrying
                    logger.info(f"Foreign key violation detected, retrying in 1 second...")
                    time.sleep(1)
                    continue
                elif retry_count >= max_retries:
                    logger.error(f"Maximum retry attempts reached. Last error: {error_msg}")
                    raise
                else:
                    # For other errors, re-raise immediately
                    raise

        return 0  # Should not reach here, but just in case

    def get_latest_snapshot(self) -> Optional[SpotSnapshot]:
        """
        Get the latest spot snapshot.

        Returns:
            Latest SpotSnapshot instance or None
        """
        return self.db.query(SpotSnapshot).order_by(SpotSnapshot.timestamp.desc()).first()

    def get_snapshot_by_id(self, snapshot_id: int) -> Optional[SpotSnapshot]:
        """
        Get a spot snapshot by ID.

        Args:
            snapshot_id: Snapshot ID

        Returns:
            SpotSnapshot instance or None
        """
        return self.db.query(SpotSnapshot).get(snapshot_id)

    def get_pairs_by_snapshot(self, snapshot_id: int,
                              exchange_name: Optional[str] = None,
                              base_asset_symbol: Optional[str] = None,
                              quote_asset_symbol: Optional[str] = None) -> List[SpotPair]:
        """
        Get pairs for a specific snapshot with optional filters.

        Args:
            snapshot_id: Snapshot ID
            exchange_name: Filter by exchange name
            base_asset_symbol: Filter by base asset symbol
            quote_asset_symbol: Filter by quote asset symbol

        Returns:
            List of SpotPair instances
        """
        query = self.db.query(SpotPair).filter(SpotPair.snapshot_id == snapshot_id)

        if exchange_name:
            query = query.join(Exchange).filter(Exchange.name == exchange_name)

        if base_asset_symbol:
            query = query.join(Asset, SpotPair.base_asset_id == Asset.id).filter(Asset.symbol == base_asset_symbol)

        if quote_asset_symbol:
            query = query.join(Asset, SpotPair.quote_asset_id == Asset.id).filter(Asset.symbol == quote_asset_symbol)

        return query.all()

    def get_pair_by_symbol(self, snapshot_id: int, exchange_name: str, symbol: str) -> Optional[SpotPair]:
        """
        Get a spot pair by its symbol from a specific exchange and snapshot.

        Args:
            snapshot_id: Snapshot ID
            exchange_name: Exchange name
            symbol: Trading pair symbol

        Returns:
            SpotPair instance or None
        """
        return self.db.query(SpotPair).join(Exchange).filter(
            SpotPair.snapshot_id == snapshot_id,
            Exchange.name == exchange_name,
            SpotPair.symbol == symbol
        ).first()

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
            # Commit the changes to ensure they're visible to other transactions
            self.db.commit()

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
            # Commit the changes to ensure they're visible to other transactions
            self.db.commit()

            # Cache new assets
            for asset in assets_to_create:
                ASSET_CACHE.set(asset.symbol, asset)

        return result

    def _dto_to_entity(self, dto: SpotPairDTO) -> SpotPair:
        """
        Convert a DTO to an entity.

        Args:
            dto: DTO to convert

        Returns:
            Entity
        """
        # Get required references
        exchange = self._get_or_create_exchange(dto.exchange_name)
        base_asset = self._get_or_create_asset(dto.base_asset_symbol)
        quote_asset = self._get_or_create_asset(dto.quote_asset_symbol)

        # Create entity
        return SpotPair(
            exchange_id=exchange.id,
            base_asset_id=base_asset.id,
            quote_asset_id=quote_asset.id,
            symbol=dto.symbol,
            price=dto.price,
            bid_price=dto.bid_price,
            ask_price=dto.ask_price,
            volume_24h=dto.volume_24h,
            high_24h=dto.high_24h,
            low_24h=dto.low_24h
        )