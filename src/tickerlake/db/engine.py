"""Centralized database engine management for all layers. 🔧"""

from typing import Optional

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool

from tickerlake.config import settings
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)

# Singleton engine with connection pooling 🔗
_engine: Optional[Engine] = None


def get_engine() -> Engine:
    """Get or create SQLAlchemy engine with connection pooling.

    Returns:
        SQLAlchemy Engine instance (singleton, reused across all layers).
    """
    global _engine
    if _engine is None:
        _engine = create_engine(
            settings.postgres_url,
            poolclass=QueuePool,
            pool_size=5,
            max_overflow=10,
            echo=False,  # Set to True for SQL debugging
        )
        logger.info(f"🔗 Connected to Postgres at {settings.postgres_host}:{settings.postgres_port}")
    return _engine
