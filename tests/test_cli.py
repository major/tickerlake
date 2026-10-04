"""Tests for tickerlake's command-line entry point: parsing and dispatch."""

import datetime

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


@pytest.fixture
def fake_pipeline(monkeypatch: pytest.MonkeyPatch) -> _RecordingPipeline:
    """Replace the two pipeline entry points with a recording double.

    ``tickerlake.main`` looks each entry point up on the ``pipeline`` module at
    call time, so patching the attributes on the real module object is enough.
    """
    recorder = _RecordingPipeline()
    for name in ("backfill", "update"):
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
    for command in ("backfill", "update"):
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
    monkeypatch: pytest.MonkeyPatch, fake_pipeline: _RecordingPipeline
) -> None:
    """Every backfill option is forwarded into the Config handed to the pipeline."""
    _run_cli(
        monkeypatch,
        "backfill",
        "--start-date",
        "2023-01-01",
        "--end-date",
        "2024-12-31",
    )

    expected = Config(
        start_date=datetime.date(2023, 1, 1),
        end_date=datetime.date(2024, 12, 31),
    )
    assert fake_pipeline.calls == [("backfill", expected)]


def test_backfill_without_options_uses_config_defaults(
    monkeypatch: pytest.MonkeyPatch, fake_pipeline: _RecordingPipeline
) -> None:
    """Bare backfill builds a Config identical to the dataclass defaults."""
    _run_cli(monkeypatch, "backfill")

    assert fake_pipeline.calls == [("backfill", Config())]


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
