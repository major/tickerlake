"""Tests for tickerlake's command-line entry point: parsing and dispatch."""

import datetime
import logging
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

from tickerlake import main, pipeline
from tickerlake.config import Config

# argparse exits with this code for usage errors (unknown/missing arguments).
_USAGE_ERROR_EXIT_CODE = 2


class _RecordingPipeline:
    """In-memory stand-in for the pipeline entry points.

    Records every ``(command_name, config)`` pair it is called with so tests can
    assert on the real ``Config`` objects the CLI forwarded, without touching
    the network or the filesystem.
    """

    def __init__(self) -> None:
        self.calls: list[tuple[str, Config]] = []

    def backfill(self, config: Config) -> None:
        """Record a backfill dispatch."""
        self.calls.append(("backfill", config))

    def update(self, config: Config) -> None:
        """Record an update dispatch."""
        self.calls.append(("update", config))

    def info(self, config: Config) -> None:
        """Record an info dispatch."""
        self.calls.append(("info", config))

    def compact(self, config: Config) -> None:
        """Record a compact dispatch."""
        self.calls.append(("compact", config))


@pytest.fixture
def fake_pipeline(monkeypatch: pytest.MonkeyPatch) -> _RecordingPipeline:
    """Replace the four pipeline entry points with a recording double.

    ``tickerlake.main`` looks each entry point up on the ``pipeline`` module at
    call time, so patching the attributes on the real module object is enough.
    """
    recorder = _RecordingPipeline()
    for name in ("backfill", "update", "info", "compact"):
        monkeypatch.setattr(pipeline, name, getattr(recorder, name))
    return recorder


def _run_cli(monkeypatch: pytest.MonkeyPatch, *argv: str) -> None:
    """Run the CLI entry point with ``argv`` as if typed at the shell."""
    monkeypatch.setattr("sys.argv", ["tickerlake", *argv])
    main()


def test_help_lists_every_subcommand(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """The top-level help text advertises every supported subcommand."""
    with pytest.raises(SystemExit) as exc_info:
        _run_cli(monkeypatch, "--help")

    assert exc_info.value.code == 0
    captured = capsys.readouterr()
    for command in ("backfill", "update", "info", "compact"):
        assert command in captured.out


def test_missing_command_exits_with_usage_error(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Running with no subcommand is a usage error, not a silent success."""
    with pytest.raises(SystemExit) as exc_info:
        _run_cli(monkeypatch)

    assert exc_info.value.code == _USAGE_ERROR_EXIT_CODE
    captured = capsys.readouterr()
    assert "required" in captured.err


@pytest.mark.parametrize(
    ("option", "value"),
    [
        ("--start-date", "not-a-date"),
        ("--end-date", "2024/12/31"),
        ("--start-date", "2024-13-40"),
    ],
)
def test_invalid_date_option_exits_before_running_etl(
    option: str,
    value: str,
    fake_pipeline: _RecordingPipeline,
    monkeypatch: pytest.MonkeyPatch,
    capsys,
) -> None:
    """A malformed date fails parsing and never reaches the pipeline."""
    with pytest.raises(SystemExit) as exc_info:
        _run_cli(monkeypatch, "backfill", option, value)

    assert exc_info.value.code == _USAGE_ERROR_EXIT_CODE
    captured = capsys.readouterr()
    assert "Invalid date format" in captured.err
    assert fake_pipeline.calls == []


@pytest.mark.parametrize("command", ["etf-race", "ciovacco", "ciovacco-stocks", "pivots", "fib-zones"])
def test_removed_report_commands_are_rejected(command: str, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Removed report commands are not accepted by the CLI."""
    with pytest.raises(SystemExit) as exc_info:
        _run_cli(monkeypatch, command)

    assert exc_info.value.code == _USAGE_ERROR_EXIT_CODE
    captured = capsys.readouterr()
    assert f"invalid choice: '{command}'" in captured.err


def test_backfill_forwards_every_option_to_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_pipeline: _RecordingPipeline
) -> None:
    """Every backfill option is forwarded into the Config handed to the pipeline."""
    _run_cli(
        monkeypatch,
        "backfill",
        "--start-date",
        "2023-01-01",
        "--end-date",
        "2024-12-31",
        "--output-dir",
        str(tmp_path),
    )

    expected = Config(
        start_date=datetime.date(2023, 1, 1),
        end_date=datetime.date(2024, 12, 31),
        output_dir=tmp_path,
    )
    assert fake_pipeline.calls == [("backfill", expected)]


def test_backfill_without_options_uses_config_defaults(
    monkeypatch: pytest.MonkeyPatch, fake_pipeline: _RecordingPipeline
) -> None:
    """Bare backfill builds a Config identical to the dataclass defaults."""
    _run_cli(monkeypatch, "backfill")

    assert fake_pipeline.calls == [("backfill", Config())]
    _, config = fake_pipeline.calls[0]
    assert config.output_dir == Path.cwd()


def test_backfill_cli_persists_requested_range_in_selected_output_dir(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """CLI options drive a real backfill, with only Massive replaced."""

    @dataclass
    class Bar:
        timestamp: int
        ticker: str
        open: float
        high: float
        low: float
        close: float
        volume: float
        vwap: float
        transactions: int

    @dataclass
    class Ticker:
        ticker: str
        name: str
        type: str
        primary_exchange: str
        cik: str
        active: bool

    class FakeMassiveClient:
        def __init__(self, config: Config) -> None:
            assert config.api_key == "cli-test-key"
            self.requested_dates: list[datetime.date] = []

        def fetch_daily_aggs(self, date: datetime.date) -> list[Bar]:
            self.requested_dates.append(date)
            if date != datetime.date(2024, 1, 3):
                return []
            return [
                Bar(
                    timestamp=int(datetime.datetime(2024, 1, 3, tzinfo=datetime.UTC).timestamp() * 1000),
                    ticker="XYZ",
                    open=10.0,
                    high=12.0,
                    low=9.0,
                    close=11.0,
                    volume=500.0,
                    vwap=10.5,
                    transactions=7,
                )
            ]

        def fetch_splits(self, start_date: datetime.date, end_date: datetime.date) -> list:
            return []

        def fetch_tickers(self, types: list[str]) -> list[Ticker]:
            return [Ticker("XYZ", "Example Corp", "CS", "XNAS", "0000000001", True)]

    output_dir = tmp_path / "cli-output"
    output_dir.mkdir()
    client: FakeMassiveClient | None = None

    def make_client(config: Config) -> FakeMassiveClient:
        nonlocal client
        client = FakeMassiveClient(config)
        return client

    monkeypatch.setenv("MASSIVE_API_KEY", "cli-test-key")
    monkeypatch.setattr(pipeline, "MassiveClient", make_client)
    _run_cli(
        monkeypatch,
        "backfill",
        "--start-date",
        "2024-01-03",
        "--end-date",
        "2024-01-03",
        "--output-dir",
        str(output_dir),
    )

    def rows(database: str, query: str) -> list[tuple]:
        connection = duckdb.connect(str(output_dir / database), read_only=True)
        try:
            return connection.execute(query).fetchall()
        finally:
            connection.close()

    date = datetime.date(2024, 1, 3)
    assert client is not None
    assert set(client.requested_dates) == {date}
    raw_bars = rows(
        "raw.duckdb",
        "SELECT date, ticker, open, high, low, close, volume, vwap, transactions FROM raw_daily_bars",
    )
    assert raw_bars == [(date, "XYZ", 10.0, 12.0, 9.0, 11.0, 500.0, 10.5, 7)]
    assert rows("tickerlake.duckdb", "SELECT date, ticker, close, volume FROM daily_bars") == [
        (date, "XYZ", 11.0, 500.0)
    ]
    assert rows("tickerlake.duckdb", "SELECT ticker, name FROM tickers") == [("XYZ", "Example Corp")]
    assert not (tmp_path / "raw.duckdb").exists()


def test_update_forwards_output_dir_to_config(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, fake_pipeline: _RecordingPipeline
) -> None:
    """Update forwards --output-dir into the Config handed to the pipeline."""
    _run_cli(monkeypatch, "update", "--output-dir", str(tmp_path))

    assert fake_pipeline.calls == [("update", Config(output_dir=tmp_path))]


def test_info_reports_missing_databases_without_api_key(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys
) -> None:
    """Cold-start info routes missing database details to stderr without a key."""
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)
    root_logger = logging.getLogger()
    old_handlers = root_logger.handlers[:]
    old_level = root_logger.level
    root_logger.handlers.clear()
    root_logger.setLevel(logging.WARNING)

    try:
        _run_cli(monkeypatch, "info", "--output-dir", str(tmp_path))
        captured = capsys.readouterr()
    finally:
        for handler in root_logger.handlers:
            handler.close()
        root_logger.handlers.clear()
        root_logger.handlers.extend(old_handlers)
        root_logger.setLevel(old_level)

    assert captured.out == ""
    output = "".join(captured.err.split())
    assert f"raw.duckdb:notfound({tmp_path / 'raw.duckdb'})" in output
    assert f"tickerlake.duckdb:notfound({tmp_path / 'tickerlake.duckdb'})" in output


def test_compact_without_raw_db_warns_and_succeeds(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, caplog) -> None:
    """Compact needs no API key and warns without failing when raw.duckdb is absent."""
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    with caplog.at_level(logging.WARNING):
        _run_cli(monkeypatch, "compact", "--output-dir", str(tmp_path))

    assert "No raw.duckdb found" in caplog.text
    assert str(tmp_path) in caplog.text


@pytest.mark.parametrize("command", ["backfill", "update"])
def test_api_commands_without_key_exit_with_usage_error(command: str, monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    """Massive-backed commands surface a missing key as a clean usage error."""
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    with pytest.raises(SystemExit) as exc_info:
        _run_cli(monkeypatch, command)

    assert exc_info.value.code == _USAGE_ERROR_EXIT_CODE
    captured = capsys.readouterr()
    assert "MASSIVE_API_KEY environment variable is required" in captured.err
    assert "Traceback" not in captured.err
