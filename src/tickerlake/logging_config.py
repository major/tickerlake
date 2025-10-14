"""Centralized logging configuration for TickerLake."""

import logging
import sys
from rich.logging import RichHandler


def setup_logging(level: int = logging.INFO) -> None:
    """Configure logging with rich handler for the entire application.

    Args:
        level: Logging level (default: logging.INFO).
    """
    # Detect if we're running in pytest
    is_pytest = "pytest" in sys.modules

    if is_pytest:
        # Use basic handler for pytest to avoid ANSI code issues
        logging.basicConfig(
            level=level,
            format="%(asctime)s %(levelname)-8s %(message)s",
            datefmt="%H:%M:%S",
            force=True,
        )
    else:
        # Use RichHandler for normal operation
        logging.basicConfig(
            level=level,
            format="%(message)s",
            handlers=[RichHandler(rich_tracebacks=True, markup=False, show_path=False)],
            force=True,  # Override any existing configuration
        )


def get_logger(name: str) -> logging.Logger:
    """Get a logger instance for the given module name.

    Args:
        name: Name of the module (typically __name__).

    Returns:
        logging.Logger: Configured logger instance.
    """
    return logging.getLogger(name)
