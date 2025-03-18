# data_storage/base_repository.py
"""
Base repository with common database operations and batch processing capabilities.
"""
import logging
import time
from typing import List, Dict, Any, Optional, Type, TypeVar, Generic, Tuple
from datetime import datetime
import json
from contextlib import contextmanager

from sqlalchemy.orm import Session
from sqlalchemy import inspect, text, func
from sqlalchemy.ext.declarative import DeclarativeMeta
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

# Generic type variables
T = TypeVar('T', bound=DeclarativeMeta)  # SQLAlchemy model type
D = TypeVar('D')  # DTO type


class BaseRepository(Generic[T, D]):
    """
    Base repository with common database operations and batch processing.

    Generic Parameters:
        T: The SQLAlchemy model type
        D: The DTO type
    """

    def __init__(self, db_session: Session, model_class: Type[T]):
        """
        Initialize the repository.

        Args:
            db_session: SQLAlchemy database session
            model_class: SQLAlchemy model class
        """
        self.db = db_session
        self.model_class = model_class

    @contextmanager
    def transaction(self):
        """
        Context manager for transactions.

        Usage:
            with repo.transaction():
                repo.create(...)
                repo.update(...)
        """
        # Only start a new transaction if one isn't already active
        transaction_already_begun = self.db.in_transaction()

        try:
            if not transaction_already_begun:
                self.db.begin()

            yield

            # Only commit if we started the transaction
            if not transaction_already_begun:
                self.db.commit()
        except Exception as e:
            # Only rollback if we started the transaction
            if not transaction_already_begun and self.db.is_active:
                self.db.rollback()
            raise e

    def create(self, entity: T) -> T:
        """
        Create a new entity.

        Args:
            entity: Entity to create

        Returns:
            Created entity
        """
        self.db.add(entity)
        self.db.flush()
        return entity

    def update(self, entity: T) -> T:
        """
        Update an entity.

        Args:
            entity: Entity to update

        Returns:
            Updated entity
        """
        self.db.merge(entity)
        self.db.flush()
        return entity

    def delete(self, entity: T) -> None:
        """
        Delete an entity.

        Args:
            entity: Entity to delete
        """
        self.db.delete(entity)
        self.db.flush()

    def get_by_id(self, entity_id: Any) -> Optional[T]:
        """
        Get an entity by ID.

        Args:
            entity_id: Entity ID

        Returns:
            Entity or None
        """
        return self.db.query(self.model_class).get(entity_id)

    def get_all(self) -> List[T]:
        """
        Get all entities.

        Returns:
            List of entities
        """
        return self.db.query(self.model_class).all()

    def count(self) -> int:
        """
        Count entities.

        Returns:
            Entity count
        """
        return self.db.query(func.count(inspect(self.model_class).primary_key[0])).scalar()

    def exists(self, entity_id: Any) -> bool:
        """
        Check if an entity exists.

        Args:
            entity_id: Entity ID

        Returns:
            True if exists, False otherwise
        """
        query = self.db.query(func.count(inspect(self.model_class).primary_key[0])) \
            .filter(inspect(self.model_class).primary_key[0] == entity_id)
        return query.scalar() > 0

    def flush(self) -> None:
        """Flush the session."""
        self.db.flush()

    def commit(self) -> None:
        """Commit the session."""
        self.db.commit()

    def rollback(self) -> None:
        """Rollback the session."""
        self.db.rollback()

    def batch_insert(self, rows: List[Tuple], columns: List[str],
                     table_name: Optional[str] = None) -> int:
        """
        Batch insert rows using native PostgreSQL COPY.

        Args:
            rows: List of row tuples (values only)
            columns: List of column names
            table_name: Table name (defaults to model_class.__tablename__)

        Returns:
            Number of rows inserted
        """
        if not rows:
            return 0

        table = table_name or self.model_class.__tablename__

        start_time = time.time()
        logger.info(f"Starting batch insert of {len(rows)} rows into {table}")

        # Use raw connection to leverage psycopg2's optimized batch operations
        connection = self.db.bind.raw_connection()
        try:
            cursor = connection.cursor()
            try:
                # Create the placeholders for the SQL query
                columns_str = ", ".join(columns)

                # Use psycopg2's execute_values for efficient batch insert
                insert_query = f"""
                INSERT INTO {table} ({columns_str}) 
                VALUES %s
                """

                # Execute the batch insert
                execute_values(
                    cursor,
                    insert_query,
                    rows,
                    page_size=1000  # Process in chunks of 1000
                )

                # Count affected rows
                row_count = cursor.rowcount

                # The connection must be committed explicitly
                connection.commit()

                logger.info(f"Successfully inserted {row_count} rows")
                duration = time.time() - start_time
                logger.info(f"Batch insert completed in {duration:.2f} seconds")

                return row_count
            except Exception as e:
                connection.rollback()
                logger.error(f"Error during batch insert: {str(e)}")
                raise
            finally:
                cursor.close()
        finally:
            connection.close()

    def execute_raw_sql(self, sql: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute raw SQL and return results as dictionaries.

        Args:
            sql: SQL query
            params: Query parameters

        Returns:
            List of result dictionaries
        """
        result = self.db.execute(text(sql), params or {})
        column_names = result.keys()
        return [dict(zip(column_names, row)) for row in result.fetchall()]

    def _dto_to_entity(self, dto: D) -> T:
        """
        Convert a DTO to an entity.
        Should be implemented by subclasses.

        Args:
            dto: DTO to convert

        Returns:
            Entity
        """
        raise NotImplementedError("_dto_to_entity must be implemented by subclasses")

    def _entity_to_dto(self, entity: T) -> D:
        """
        Convert an entity to a DTO.
        Should be implemented by subclasses.

        Args:
            entity: Entity to convert

        Returns:
            DTO
        """
        raise NotImplementedError("_entity_to_dto must be implemented by subclasses")