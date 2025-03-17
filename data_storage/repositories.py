# data_storage/repositories.py
import json
import logging
import time
from datetime import datetime

from pandas.core.groupby.base import transform_kernel_allowlist
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Dict, Any, Optional
import threading
from sqlalchemy import inspect

from core.models import Exchange, Asset, Fiat, P2PSnapshot, SpotSnapshot, P2POrder, SpotPair
from core.dto import P2POrderDTO, SpotPairDTO
from config.exchanges import ASSETS, EXCHANGE_SETTINGS
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

_asset_cache_lock = threading.RLock()
_asset_cache = {}

_fiat_cache_lock = threading.RLock()
_fiat_cache = {}


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
        "UZS": "Uzbekistani Som"
        # Add other currencies as needed
    }
    return fiat_names.get(code, code)


def get_or_create_assets_safe(db_session: Session, asset_symbols: set) -> Dict[str, Asset]:
    """
    Thread-safe implementation to get or create assets.
    Uses a global cache with proper locking to prevent conflicts.
    """
    if not asset_symbols:
        return {}

    result = {}
    missing_symbols = []

    # First check the cache (with lock)
    with _asset_cache_lock:
        for symbol in asset_symbols:
            if symbol in _asset_cache:
                result[symbol] = _asset_cache[symbol]
            else:
                missing_symbols.append(symbol)

    if not missing_symbols:
        return result  # All found in cache

    # Query the database for existing assets
    try:
        existing_assets = db_session.query(Asset).filter(
            Asset.symbol.in_(missing_symbols)
        ).all()

        # Update result and cache with found assets
        with _asset_cache_lock:
            for asset in existing_assets:
                result[asset.symbol] = asset
                _asset_cache[asset.symbol] = asset

        # Find truly missing assets
        still_missing = set(missing_symbols) - set(asset.symbol for asset in existing_assets)

        if still_missing:
            # Create the missing assets with direct SQL for maximum safety
            # Get the raw connection properly
            connection = db_session.bind.raw_connection()
            try:
                cursor = connection.cursor()
                try:
                    # First, try to insert all missing assets at once
                    inserted_rows = []
                    for symbol in still_missing:
                        # Use the symbol as name too for simplicity
                        inserted_rows.append((symbol, symbol, datetime.now()))

                    insert_query = """
                    INSERT INTO assets (symbol, name, created_at) 
                    VALUES %s 
                    ON CONFLICT (symbol) DO NOTHING
                    RETURNING id, symbol, name, created_at;
                    """

                    execute_values(cursor, insert_query, inserted_rows, page_size=100)
                    created_assets = cursor.fetchall() or []

                    # Now fetch any assets that already existed (the conflict case)
                    conflict_symbols = still_missing - set(row[1] for row in created_assets if len(row) > 1)
                    if conflict_symbols:
                        placeholders = ','.join(['%s'] * len(conflict_symbols))
                        conflict_query = f"""
                        SELECT id, symbol, name, created_at FROM assets
                        WHERE symbol IN ({placeholders})
                        """
                        cursor.execute(conflict_query, list(conflict_symbols))
                        conflict_assets = cursor.fetchall() or []

                        # Combine created and conflict-resolved assets
                        all_assets = created_assets + conflict_assets
                    else:
                        all_assets = created_assets

                    # Commit the changes
                    connection.commit()

                    # Now create proper SQLAlchemy objects with the IDs from the database
                    for row in all_assets:
                        if len(row) >= 2:  # Make sure we have enough elements
                            id, symbol = row[0], row[1]

                            # Create the Asset object and add to session without flush
                            asset = Asset(id=id, symbol=symbol, name=symbol)
                            db_session.merge(asset)

                            # Update result and cache
                            with _asset_cache_lock:
                                result[symbol] = asset
                                _asset_cache[symbol] = asset
                finally:
                    cursor.close()
            finally:
                connection.close()

    except Exception as e:
        logger.error(f"Error in get_or_create_assets_safe: {e}")
        if not db_session.is_active:
            db_session.rollback()
        raise

    return result

class P2PRepository:
    """Repository for managing P2P market data."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
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
        
        snapshot = P2PSnapshot(timestamp=timestamp)
        self.db.add(snapshot)
        self.db.commit()
        self.db.refresh(snapshot)
        
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
        # Get or create Exchange
        exchange = self.db.query(Exchange).filter_by(name=order_data.exchange_name).first()
        if not exchange:
            exchange_settings = next(
                (s for k, s in EXCHANGE_SETTINGS.items() 
                 if k.lower() == order_data.exchange_name.lower() or 
                    s['base_url'].find(order_data.exchange_name.lower()) != -1),
                {}
            )
            exchange = Exchange(
                name=order_data.exchange_name,
                base_url=exchange_settings.get('base_url', ''),
                p2p_url=exchange_settings.get('p2p_url', '')
            )
            self.db.add(exchange)
            self.db.commit()
            self.db.refresh(exchange)
        
        # Get or create Asset
        asset = self.db.query(Asset).filter_by(symbol=order_data.asset_symbol).first()
        if not asset:
            asset = Asset(
                symbol=order_data.asset_symbol,
                name=order_data.asset_symbol
            )
            self.db.add(asset)
            self.db.commit()
            self.db.refresh(asset)

        # Get or create Fiat
        fiat = self.db.query(Fiat).filter_by(code=order_data.fiat_code).first()
        if not fiat:
            fiat = Fiat(
                code=order_data.fiat_code,
                name=order_data.fiat_code
            )
            self.db.add(fiat)
            self.db.commit()
            self.db.refresh(fiat)
        
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
            order_id=order_data.order_id,
            user_id=order_data.user_id,
            user_name=order_data.user_name,
            completion_rate=order_data.completion_rate
        )
        
        self.db.add(order)
        self.db.commit()
        self.db.refresh(order)
        
        return order

    def add_orders_batch_postgresql(self, snapshot: P2PSnapshot, orders: List[P2POrderDTO]) -> None:
        """Thread-safe optimized batch insertion for PostgreSQL."""
        if not orders:
            return

        start_time = time.time()
        logger.info(f"Starting bulk insert of {len(orders)} P2P orders")

        # Check if we're already in a transaction
        transaction_already_begun = self.db.in_transaction()

        try:
            # Only start a new transaction if one isn't already active
            if not transaction_already_begun:
                self.db.begin()

            # First ensure all reference entities exist
            exchange_map = self._prepare_exchanges(orders)
            asset_map = get_or_create_assets_safe(self.db, set(order.asset_symbol for order in orders))
            fiat_map = self._prepare_fiats(orders)

            # Ensure we have all needed references before proceeding
            valid_orders = []
            for order in orders:
                if order.exchange_name not in exchange_map:
                    logger.warning(f"Missing exchange {order.exchange_name} for order, skipping")
                    continue
                if order.asset_symbol not in asset_map:
                    logger.warning(f"Missing asset {order.asset_symbol} for order, skipping")
                    continue
                if order.fiat_code not in fiat_map:
                    logger.warning(f"Missing fiat {order.fiat_code} for order, skipping")
                    continue

                valid_orders.append((
                    exchange_map[order.exchange_name].id,
                    asset_map[order.asset_symbol].id,
                    fiat_map[order.fiat_code].id,
                    snapshot.id,
                    order.price or 0,
                    order.order_type,
                    order.available_amount or 0,
                    order.min_amount or 0,
                    order.max_amount or 0,
                    json.dumps(order.payment_methods or []),
                    order.order_id,
                    order.user_id,
                    order.user_name,
                    order.completion_rate or 0,
                    datetime.now()
                ))

            if not valid_orders:
                logger.warning("No valid orders to insert after filtering")
                if not transaction_already_begun:
                    self.db.commit()
                return

            # Use raw connection method that works with SQLAlchemy
            connection = self.db.bind.raw_connection()
            try:
                cursor = connection.cursor()
                try:
                    # Using psycopg2 batch mode for optimal performance
                    insert_query = """
                    INSERT INTO p2p_orders (
                        exchange_id, asset_id, fiat_id, snapshot_id, price, order_type,
                        available_amount, min_amount, max_amount, payment_methods,
                        order_id, user_id, user_name, completion_rate, created_at
                    ) VALUES %s
                    """

                    # Execute the batch insert
                    from psycopg2.extras import execute_values
                    execute_values(
                        cursor,
                        insert_query,
                        valid_orders,
                        template="(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        page_size=1000  # Process in chunks of 1000
                    )

                    # We don't commit the connection here because we'll commit the SQLAlchemy transaction
                finally:
                    cursor.close()

                # If we started the transaction, we commit it
                if not transaction_already_begun:
                    self.db.commit()
                # Otherwise we leave it to the caller to commit

                logger.info(f"Successfully inserted {len(valid_orders)} P2P orders")
            finally:
                connection.close()

            duration = time.time() - start_time
            logger.info(f"Completed bulk insert of P2P orders in {duration:.2f} seconds")

        except Exception as e:
            logger.error(f"Error during batch insert of P2P orders: {e}")
            # Only rollback if we started the transaction
            if not transaction_already_begun and self.db.is_active:
                self.db.rollback()
            raise

    def _prepare_exchanges(self, orders: List[P2POrderDTO]) -> Dict[str, Exchange]:
        # Extract unique exchange names from all orders
        exchange_names = set(order.exchange_name for order in orders)

        # Fetch all existing exchanges in a single query
        existing_exchanges = self.db.query(Exchange).filter(
            Exchange.name.in_(exchange_names)
        ).all()

        # Create a mapping of name -> exchange entity
        exchange_map = {exchange.name: exchange for exchange in existing_exchanges}

        # Check which exchanges need to be created
        exchanges_to_create = []
        for name in exchange_names:
            if name not in exchange_map:
                # Find exchange settings if available
                exchange_settings = next(
                    (s for k, s in EXCHANGE_SETTINGS.items()
                     if k.lower() == name.lower() or
                     s.get('base_url', '').find(name.lower()) != -1),
                    {}
                )

                # Create a new exchange entity
                exchange = Exchange(
                    name=name,
                    base_url=exchange_settings.get('base_url', ''),
                    p2p_url=exchange_settings.get('p2p_url', ''),
                    fiat_currencies=exchange_settings.get('fiat_currencies', []),
                )
                exchanges_to_create.append(exchange)
                exchange_map[name] = exchange

        # Bulk insert all new exchanges at once
        if exchanges_to_create:
            self.db.add_all(exchanges_to_create)
            self.db.flush()  # Get IDs without committing transaction

        return exchange_map

    def _prepare_assets(self, pairs: List[P2POrderDTO]) -> Dict[str, P2POrder]:
        """Thread-safe method to prepare assets for spot pairs."""
        # Collect all unique asset symbols
        asset_symbols = set(pair.asset_symbol for pair in pairs)

        # Use our thread-safe function
        return get_or_create_assets_safe(self.db, asset_symbols)

    def _prepare_fiats(self, orders: List[P2POrderDTO]) -> Dict[str, Fiat]:
        """Thread-safe method to prepare fiat currencies."""
        # Extract unique fiat codes
        fiat_codes = set(order.fiat_code for order in orders if order.fiat_code)

        # Add USD as default if None is present
        if None in fiat_codes:
            fiat_codes.remove(None)
            fiat_codes.add("USD")

        result = {}
        missing_codes = []

        # First check the cache (with lock)
        with _fiat_cache_lock:
            for code in fiat_codes:
                if code in _fiat_cache:
                    result[code] = _fiat_cache[code]
                else:
                    missing_codes.append(code)

        if not missing_codes:
            return result  # All found in cache

        # Query the database for existing fiats
        existing_fiats = self.db.query(Fiat).filter(Fiat.code.in_(missing_codes)).all()

        # Update result and cache with found fiats
        with _fiat_cache_lock:
            for fiat in existing_fiats:
                result[fiat.code] = fiat
                _fiat_cache[fiat.code] = fiat

        # Find truly missing fiats
        still_missing = set(missing_codes) - set(fiat.code for fiat in existing_fiats)

        if still_missing:
            # Create the missing fiats
            fiats_to_create = []
            for code in still_missing:
                fiat = Fiat(code=code, name=get_fiat_name(code))
                fiats_to_create.append(fiat)
                result[code] = fiat
                with _fiat_cache_lock:
                    _fiat_cache[code] = fiat

            # Add and flush to get IDs
            self.db.add_all(fiats_to_create)
            self.db.flush()

        return result




class SpotRepository:
    """Repository for managing spot market data."""
    
    def __init__(self, db_session: Session):
        self.db = db_session
    
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
        
        snapshot = SpotSnapshot(timestamp=timestamp)
        self.db.add(snapshot)
        self.db.commit()
        self.db.refresh(snapshot)
        
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
        # Get or create Exchange
        exchange = self.db.query(Exchange).filter_by(name=pair_data.exchange_name).first()
        if not exchange:
            exchange_settings = next(
                (s for k, s in EXCHANGE_SETTINGS.items() 
                 if k.lower() == pair_data.exchange_name.lower() or 
                    s['base_url'].find(pair_data.exchange_name.lower()) != -1),
                {}
            )
            exchange = Exchange(
                name=pair_data.exchange_name,
                base_url=exchange_settings.get('base_url', ''),
                p2p_url=exchange_settings.get('p2p_url', '')
            )
            self.db.add(exchange)
            self.db.commit()
            self.db.refresh(exchange)
        
        # Get or create Base Asset
        base_asset = self.db.query(Asset).filter_by(symbol=pair_data.base_asset_symbol).first()
        if not base_asset:
            base_asset = Asset(
                symbol=pair_data.base_asset_symbol,
                name=pair_data.base_asset_symbol
            )
            self.db.add(base_asset)
            self.db.commit()
            self.db.refresh(base_asset)
        
        # Get or create Quote Asset
        quote_asset = self.db.query(Asset).filter_by(symbol=pair_data.quote_asset_symbol).first()
        if not quote_asset:
            quote_asset = Asset(
                symbol=pair_data.quote_asset_symbol,
                name=pair_data.quote_asset_symbol
            )
            self.db.add(quote_asset)
            self.db.commit()
            self.db.refresh(quote_asset)
        
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
        self.db.commit()
        self.db.refresh(pair)
        
        return pair

    def add_pairs_batch_postgresql(self, snapshot: SpotSnapshot, pairs: List[SpotPairDTO]) -> None:
        """Thread-safe optimized batch insertion for PostgreSQL."""
        if not pairs:
            return

        start_time = time.time()
        logger.info(f"Starting bulk insert of {len(pairs)} pairs")

        # Check if we're already in a transaction
        transaction_already_begun = self.db.in_transaction()

        try:
            # Only start a new transaction if one isn't already active
            if not transaction_already_begun:
                self.db.begin()

            # First ensure all assets exist
            base_asset_symbols = set(pair.base_asset_symbol for pair in pairs)
            quote_asset_symbols = set(pair.quote_asset_symbol for pair in pairs)
            all_asset_symbols = base_asset_symbols.union(quote_asset_symbols)

            # Thread-safe asset creation
            asset_map = get_or_create_assets_safe(self.db, all_asset_symbols)

            # Get exchanges (simpler than assets since there are fewer)
            exchange_names = set(pair.exchange_name for pair in pairs)
            exchanges = self.db.query(Exchange).filter(Exchange.name.in_(exchange_names)).all()
            exchange_map = {ex.name: ex for ex in exchanges}

            # Create any missing exchanges
            missing_exchanges = exchange_names - set(exchange_map.keys())
            if missing_exchanges:
                for name in missing_exchanges:
                    # Find settings if available
                    exchange_settings = next(
                        (s for k, s in EXCHANGE_SETTINGS.items()
                         if k.lower() == name.lower() or
                         s.get('base_url', '').find(name.lower()) != -1),
                        {}
                    )
                    exchange = Exchange(
                        name=name,
                        base_url=exchange_settings.get('base_url', ''),
                        p2p_url=exchange_settings.get('p2p_url', ''),
                        fiat_currencies=exchange_settings.get('fiat_currencies', ["USD"])
                    )
                    self.db.add(exchange)

                self.db.flush()  # Get IDs

                # Refresh exchange map
                exchanges = self.db.query(Exchange).filter(Exchange.name.in_(exchange_names)).all()
                exchange_map = {ex.name: ex for ex in exchanges}

            # Ensure we have all needed references before proceeding
            valid_pairs = []
            for pair in pairs:
                if pair.base_asset_symbol not in asset_map:
                    logger.warning(f"Missing base asset {pair.base_asset_symbol} for pair {pair.symbol}, skipping")
                    continue
                if pair.quote_asset_symbol not in asset_map:
                    logger.warning(f"Missing quote asset {pair.quote_asset_symbol} for pair {pair.symbol}, skipping")
                    continue
                if pair.exchange_name not in exchange_map:
                    logger.warning(f"Missing exchange {pair.exchange_name} for pair {pair.symbol}, skipping")
                    continue

                valid_pairs.append((
                    exchange_map[pair.exchange_name].id,
                    asset_map[pair.base_asset_symbol].id,
                    asset_map[pair.quote_asset_symbol].id,
                    snapshot.id,
                    pair.symbol,
                    pair.price or 0,  # Handle None values
                    pair.bid_price or 0,
                    pair.ask_price or 0,
                    pair.volume_24h or 0,
                    pair.high_24h or 0,
                    pair.low_24h or 0,
                    datetime.now()
                ))

            if not valid_pairs:
                logger.warning("No valid pairs to insert after filtering")
                if not transaction_already_begun:
                    self.db.commit()
                return

            # Use raw connection method that works with SQLAlchemy
            connection = self.db.bind.raw_connection()
            try:
                cursor = connection.cursor()
                try:
                    insert_query = """
                    INSERT INTO spot_pairs (
                        exchange_id, base_asset_id, quote_asset_id, snapshot_id,
                        symbol, price, bid_price, ask_price,
                        volume_24h, high_24h, low_24h, created_at
                    ) VALUES %s
                    """
                    execute_values(cursor, insert_query, valid_pairs, page_size=1000)
                    # We don't commit here because we'll commit the SQLAlchemy transaction
                finally:
                    cursor.close()

                # If we started the transaction, we commit it
                if not transaction_already_begun:
                    self.db.commit()
                # Otherwise we leave it to the caller to commit

                logger.info(f"Successfully inserted {len(valid_pairs)} pairs")
            finally:
                connection.close()

            duration = time.time() - start_time
            logger.info(f"Completed bulk insert of SpotPairs in {duration:.2f} seconds")

        except Exception as e:
            logger.error(f"Error during batch insert of pairs: {e}")
            # Only rollback if we started the transaction
            if not transaction_already_begun and self.db.is_active:
                self.db.rollback()
            raise

    def _prepare_exchanges(self, pairs: List[SpotPairDTO]) -> Dict[str, Exchange]:
        # Extract unique exchange names from all pairs
        exchange_names = set(pair.exchange_name for pair in pairs)

        # Fetch all existing exchanges in a single query
        existing_exchanges = self.db.query(Exchange).filter(
            Exchange.name.in_(exchange_names)
        ).all()

        # Create a mapping of name -> exchange entity
        exchange_map = {exchange.name: exchange for exchange in existing_exchanges}

        # Check which exchanges need to be created
        exchanges_to_create = []
        for name in exchange_names:
            if name not in exchange_map:
                # Find exchange settings if available
                exchange_settings = next(
                    (s for k, s in EXCHANGE_SETTINGS.items()
                     if k.lower() == name.lower() or
                     s.get('base_url', '').find(name.lower()) != -1),
                    {}
                )

                # Create a new exchange entity
                exchange = Exchange(
                    name=name,
                    base_url=exchange_settings.get('base_url', ''),
                    p2p_url=exchange_settings.get('p2p_url', ''),
                    fiat_currencies=exchange_settings.get('fiat_currencies', ["USD"]),
                )
                exchanges_to_create.append(exchange)
                exchange_map[name] = exchange

        # Bulk insert all new exchanges at once
        if exchanges_to_create:
            self.db.add_all(exchanges_to_create)
            self.db.flush()  # Get IDs without committing transaction

        return exchange_map

    def _prepare_assets(self, pairs: List[SpotPairDTO]) -> Dict[str, Asset]:
        """Thread-safe method to prepare assets for spot pairs."""
        # Collect all unique asset symbols
        base_asset_symbols = set(pair.base_asset_symbol for pair in pairs)
        quote_asset_symbols = set(pair.quote_asset_symbol for pair in pairs)
        all_asset_symbols = base_asset_symbols.union(quote_asset_symbols)

        # Use our thread-safe function
        return get_or_create_assets_safe(self.db, all_asset_symbols)


    def get_latest_snapshot(self) -> Optional[SpotSnapshot]:
        """
        Get the latest spot snapshot.
        
        Returns:
            Latest SpotSnapshot instance or None
        """
        return self.db.query(SpotSnapshot).order_by(SpotSnapshot.timestamp.desc()).first()