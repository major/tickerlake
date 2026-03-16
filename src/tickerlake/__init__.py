"""Tickerlake: US equity market data ETL pipeline."""

import argparse
import datetime
from pathlib import Path

from tickerlake.config import Config
from tickerlake import pipeline


def _parse_date(s: str) -> datetime.date:
    """Parse ISO date string, raising argparse.ArgumentTypeError on failure."""
    try:
        return datetime.date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date format: {s!r}. Use YYYY-MM-DD.")


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
    backfill_parser.add_argument("--output-dir", type=Path, metavar="DIR")

    update_parser = subparsers.add_parser("update", help="Incremental update")
    update_parser.add_argument("--output-dir", type=Path, metavar="DIR")

    info_parser = subparsers.add_parser("info", help="Show database info")
    info_parser.add_argument("--output-dir", type=Path, metavar="DIR")

    compact_parser = subparsers.add_parser(
        "compact", help="Rebuild raw.duckdb to reclaim space"
    )
    compact_parser.add_argument("--output-dir", type=Path, metavar="DIR")

    return parser


def _make_config(args: argparse.Namespace) -> Config:
    """Build Config from parsed CLI args, using defaults for unspecified values."""
    kwargs = {}
    if hasattr(args, "start_date") and args.start_date is not None:
        kwargs["start_date"] = args.start_date
    if hasattr(args, "end_date") and args.end_date is not None:
        kwargs["end_date"] = args.end_date
    if args.output_dir is not None:
        kwargs["output_dir"] = args.output_dir
    return Config(**kwargs)


def main() -> None:
    """Parse CLI arguments and dispatch to appropriate pipeline function."""
    parser = _build_parser()
    args = parser.parse_args()
    config = _make_config(args)

    if args.command == "backfill":
        pipeline.backfill(config)
    elif args.command == "update":
        pipeline.update(config)
    elif args.command == "info":
        pipeline.info(config)
    elif args.command == "compact":
        pipeline.compact(config)
