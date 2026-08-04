"""Tickerlake: US equity market data ETL pipeline."""

import argparse
import datetime
import logging
from pathlib import Path

from rich.console import Console
from rich.logging import RichHandler

# console must be defined before `from tickerlake import pipeline` so that
# extract.py can safely do `from tickerlake import console` without hitting a
# partially-initialised package (circular import).
console = Console(stderr=True)

from tickerlake import pipeline  # noqa: E402
from tickerlake.config import Config  # noqa: E402


def _parse_date(s: str) -> datetime.date:
    """Parse ISO date string, raising argparse.ArgumentTypeError on failure."""
    try:
        return datetime.date.fromisoformat(s)
    except ValueError as err:
        msg = f"Invalid date format: {s!r}. Use YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg) from err


def _parse_positive_int(s: str) -> int:
    """Parse a positive integer, raising argparse.ArgumentTypeError on failure."""
    try:
        value = int(s)
    except ValueError as err:
        msg = f"Invalid positive integer: {s!r}."
        raise argparse.ArgumentTypeError(msg) from err
    if value < 1:
        msg = "Value must be >= 1."
        raise argparse.ArgumentTypeError(msg)
    return value


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

    pivots_parser = subparsers.add_parser(
        "pivots", help="Find confirmed pivots for one ticker"
    )
    pivots_parser.add_argument("ticker", help="Ticker symbol, e.g. AAPL")
    pivots_parser.add_argument(
        "--timeframe",
        choices=["daily", "weekly", "monthly"],
        default="weekly",
        help="Bar timeframe for pivot detection (default: weekly)",
    )
    pivots_parser.add_argument(
        "--k",
        type=_parse_positive_int,
        default=4,
        help="Bars on each side required to confirm a pivot (default: 4)",
    )
    pivots_parser.add_argument("--output-dir", type=Path, metavar="DIR")

    fib_zones_parser = subparsers.add_parser(
        "fib-zones",
        help="Compute and screen weekly Fibonacci-retracement IBZ/SMZ zones",
    )
    fib_zones_subparsers = fib_zones_parser.add_subparsers(
        dest="fib_zones_command", metavar="COMMAND"
    )
    fib_zones_subparsers.required = True

    fib_zones_compute_parser = fib_zones_subparsers.add_parser(
        "compute", help="Compute and persist weekly fib zones for all eligible tickers"
    )
    fib_zones_compute_parser.add_argument("--output-dir", type=Path, metavar="DIR")

    fib_zones_screen_parser = fib_zones_subparsers.add_parser(
        "screen", help="Screen persisted weekly fib zones"
    )
    fib_zones_screen_parser.add_argument(
        "--zone",
        choices=["in_ibz", "in_smz", "below_smz", "above_ibz", "all"],
        default="all",
        help="Zone to filter on (default: all actionable zones)",
    )
    fib_zones_screen_parser.add_argument(
        "--min-swing-low",
        type=float,
        default=5.0,
        metavar="DOLLARS",
        help="Minimum swing low price to include (default: 5.0; use 0 to disable)",
    )
    fib_zones_screen_parser.add_argument(
        "--limit",
        type=_parse_positive_int,
        default=None,
        help="Cap the number of rows displayed",
    )
    fib_zones_screen_parser.add_argument("--output-dir", type=Path, metavar="DIR")

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


def _dispatch_fib_zones(
    parser: argparse.ArgumentParser, args: argparse.Namespace, config: Config
) -> None:
    """Dispatch the fib-zones compute/screen subcommands, wrapping ValueErrors."""
    if args.fib_zones_command == "compute":
        try:
            pipeline.compute_weekly_fib_zones(config)
        except ValueError as err:
            parser.error(str(err))
    elif args.fib_zones_command == "screen":
        try:
            pipeline.screen_fib_zones(
                config,
                zone=args.zone,
                limit=args.limit,
                min_swing_low=args.min_swing_low,
            )
        except ValueError as err:
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

    if args.command == "backfill":
        try:
            pipeline.backfill(config)
        except ValueError as err:
            parser.error(str(err))
    elif args.command == "update":
        try:
            pipeline.update(config)
        except ValueError as err:
            parser.error(str(err))
    elif args.command == "info":
        pipeline.info(config)
    elif args.command == "compact":
        pipeline.compact(config)
    elif args.command == "pivots":
        try:
            pipeline.pivots(config, args.ticker, args.timeframe, args.k)
        except ValueError as err:
            parser.error(str(err))
    elif args.command == "fib-zones":
        _dispatch_fib_zones(parser, args, config)
