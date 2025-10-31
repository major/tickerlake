"""Storage layer for Parquet files on local filesystem."""

from tickerlake.storage.checkpoints import load_checkpoints, save_checkpoints
from tickerlake.storage.operations import (
    get_max_date,
    init_table,
    read_table,
    table_exists,
    write_table,
)
from tickerlake.storage.paths import get_checkpoint_path, get_table_path

__all__ = [
    # Paths
    "get_table_path",
    "get_checkpoint_path",
    # Checkpoints
    "load_checkpoints",
    "save_checkpoints",
    # Operations
    "write_table",
    "read_table",
    "table_exists",
    "init_table",
    "get_max_date",
]
