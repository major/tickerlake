"""Shared database utilities for all TickerLake layers. 🗄️

This module provides centralized database operations to eliminate code duplication
across bronze, silver, and gold layers.

Main exports:
- get_engine(): Get singleton SQLAlchemy engine with connection pooling
- bulk_load(): Generic bulk insert using psycopg COPY BINARY (1-2M rows/sec)
- init_schema(): Initialize database tables from SQLAlchemy metadata
- execute_query(): Execute queries with simple result handling
- clear_table(): Truncate a table efficiently
- drop_schema(): Drop all tables in a metadata object

Example usage:
    >>> from tickerlake.db import bulk_load, init_schema
    >>> from tickerlake.bronze.models import metadata, stocks
    >>>
    >>> # Initialize schema
    >>> init_schema(metadata, "bronze")
    >>>
    >>> # Load data efficiently
    >>> bulk_load(stocks, df)
"""

from tickerlake.db.engine import get_engine
from tickerlake.db.operations import bulk_load, clear_table, execute_query
from tickerlake.db.schema import drop_schema, init_schema

__all__ = [
    "get_engine",
    "bulk_load",
    "init_schema",
    "execute_query",
    "clear_table",
    "drop_schema",
]
