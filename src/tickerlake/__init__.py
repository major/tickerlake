"""Tickerlake: US equity market data ETL pipeline."""

import argparse
import datetime
import logging

from rich.console import Console
from rich.logging import RichHandler

# console must be defined before `from tickerlake import pipeline` so that
# extract.py can safely do `from tickerlake import console` without hitting a
# partially-initialised package (circular import).
console = Console(stderr=True)

from tickerlake import pipeline  # noqa: E402
from tickerlake.config import Config  # noqa: E402
from tickerlake.postgres.backfill import BackfillError  # noqa: E402


def _parse_date(s: str) -> datetime.date:
    """Parse ISO date string, raising argparse.ArgumentTypeError on failure."""
    try:
        return datetime.date.fromisoformat(s)
    except ValueError as err:
        msg = f"Invalid date format: {s!r}. Use YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg) from err


def _build_parser() -> argparse.ArgumentParser:
    """Build and return the argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="tickerlake",
        description="US equity market data ETL pipeline",
    )
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND")
    subparsers.required = True

    backfill_parser = subparsers.add_parser("backfill", help="Full historical backfill")
    backfill_parser.add_argument("--start-date", type=_parse_date, metavar="YYYY-MM-DD")
    backfill_parser.add_argument("--end-date", type=_parse_date, metavar="YYYY-MM-DD")

    subparsers.add_parser("update", help="Incremental update")

    return parser


def _make_config(args: argparse.Namespace) -> Config:
    """Build Config from parsed CLI args, using defaults for unspecified values."""
    kwargs = {}
    if hasattr(args, "start_date") and args.start_date is not None:
        kwargs["start_date"] = args.start_date
    if hasattr(args, "end_date") and args.end_date is not None:
        kwargs["end_date"] = args.end_date
    return Config(**kwargs)


def _dispatch_etl(parser: argparse.ArgumentParser, config: Config, command: str) -> None:
    """Dispatch backfill/update to the pipeline, wrapping expected errors."""
    try:
        {"backfill": pipeline.backfill, "update": pipeline.update}[command](config)
    except (ValueError, BackfillError) as err:
        parser.error(str(err))


def main() -> None:
    """Parse CLI arguments and dispatch to appropriate pipeline function."""
    parser = _build_parser()
    args = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        handlers=[RichHandler(console=console, show_path=False)],
        format="%(message)s",
        datefmt="[%X]",
    )
    config = _make_config(args)

    if args.command in {"backfill", "update"}:
        _dispatch_etl(parser, config, args.command)
