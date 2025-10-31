"""Checkpoint tracking for incremental processing."""

import json
import logging
from datetime import datetime
from pathlib import Path

from tickerlake.config import settings

logger = logging.getLogger(__name__)


def load_checkpoints() -> dict:
    """
    Load checkpoint state from local filesystem.

    Returns:
        Dictionary with checkpoint data:
        - bronze_stocks_last_date: Last date processed from bronze stocks
        - silver_last_full_rewrite: Last date of full silver rewrite
        - last_run_timestamp: Timestamp of last pipeline run

    Example:
        >>> checkpoints = load_checkpoints()
        >>> checkpoints['bronze_stocks_last_date']
        '2025-10-28'
    """
    defaults = {
        "bronze_stocks_last_date": None,
        "silver_last_full_rewrite": None,
        "last_run_timestamp": None,
    }

    try:
        checkpoint_path = Path(settings.checkpoint_path)
        if checkpoint_path.exists():
            checkpoints = json.loads(checkpoint_path.read_text())
            logger.debug(f"📖 Loaded checkpoints from {checkpoint_path}: {checkpoints}")
            return checkpoints
        else:
            logger.info("ℹ️  No checkpoints found, using defaults (first run)")
            return defaults
    except Exception as e:
        logger.warning(f"⚠️  Failed to load checkpoints: {e}, using defaults")
        return defaults


def save_checkpoints(checkpoints: dict) -> None:
    """
    Save checkpoint state to local filesystem.

    Args:
        checkpoints: Dictionary with checkpoint data to save

    Example:
        >>> checkpoints = load_checkpoints()
        >>> checkpoints['bronze_stocks_last_date'] = '2025-10-29'
        >>> save_checkpoints(checkpoints)
    """
    try:
        # Add timestamp
        checkpoints["last_run_timestamp"] = datetime.now().isoformat()

        checkpoint_path = Path(settings.checkpoint_path)
        checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
        checkpoint_path.write_text(json.dumps(checkpoints, indent=2))
        logger.debug(f"💾 Saved checkpoints to {checkpoint_path}: {checkpoints}")
    except Exception as e:
        logger.error(f"❌ Failed to save checkpoints: {e}")
        raise
