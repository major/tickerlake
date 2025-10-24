"""Database schema initialization and management. 🗄️"""

from sqlalchemy import MetaData

from tickerlake.db.engine import get_engine
from tickerlake.logging_config import get_logger

logger = get_logger(__name__)


def init_schema(metadata: MetaData, layer_name: str = "") -> None:
    """Create all tables in metadata if they don't exist (idempotent).

    Args:
        metadata: SQLAlchemy MetaData object containing table definitions.
        layer_name: Optional layer name for logging (e.g., "bronze", "silver").

    Example:
        >>> from tickerlake.bronze.models import metadata
        >>> init_schema(metadata, "bronze")
    """
    engine = get_engine()
    metadata.create_all(engine)

    layer_msg = f"{layer_name} layer " if layer_name else ""
    logger.info(f"✅ Database schema initialized ({layer_msg}{len(metadata.tables)} tables)")


def drop_schema(metadata: MetaData, layer_name: str = "") -> None:
    """Drop all tables in metadata (⚠️ DESTRUCTIVE!).

    Args:
        metadata: SQLAlchemy MetaData object containing table definitions.
        layer_name: Optional layer name for logging (e.g., "bronze", "silver").

    Example:
        >>> from tickerlake.bronze.models import metadata
        >>> drop_schema(metadata, "bronze")
    """
    engine = get_engine()

    layer_msg = f"{layer_name} layer " if layer_name else ""
    logger.warning(f"⚠️  Dropping all {layer_msg}tables...")

    metadata.drop_all(engine)

    logger.info(f"✅ All {layer_msg}tables dropped")
