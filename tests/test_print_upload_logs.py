from typing import TYPE_CHECKING

import click
import pytest

from latch_cli.services.register.register import print_upload_logs

if TYPE_CHECKING:
    from latch_cli.services.register.utils import DockerPushLogItem


def test_progress_stream_does_not_exit(capsys: pytest.CaptureFixture[str]):
    """A stream with no error must run to completion, and render what it read."""
    logs: list[DockerPushLogItem] = [
        {"id": "layer_a", "progress": "1/2"},
        {"id": "layer_a", "progress": "2/2"},
        {"id": "layer_b", "progress": "1/1"},
        {"status": "Preparing"},  # type: ignore[typeddict-unknown-key]  # no id
    ]

    print_upload_logs(logs, "test_image")

    out = capsys.readouterr().out
    assert "layer_a ~ 2/2" in out
    assert "layer_b ~ 1/1" in out
    assert "None ~" not in out


def test_pull_path_reports_errors_too(capsys: pytest.CaptureFixture[str]):
    """The pull stream uses the same parser, with the header suppressed."""
    error = "manifest for barcode-analysis/barcode-tools:v1 not found"

    with pytest.raises(click.exceptions.Exit) as excinfo:
        print_upload_logs([{"error": error}], "test_image", print_header=False)

    assert excinfo.value.exit_code == 1

    out = capsys.readouterr().out
    assert error in out
    assert "Uploading Docker image" not in out


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        ("denied: Your authorization token has expired.", "is expired"),
        ("denied: requested access to the resource is denied", "denied: requested"),
        ("received unexpected HTTP status: 500 Internal Server Error", "500"),
        ("unauthorized: authentication required", "unauthorized"),
    ],
)
def test_every_error_exits_1_and_is_shown(
    error: str, expected: str, capsys: pytest.CaptureFixture[str]
):
    """Every error is fatal, not just the expired-token case."""
    with pytest.raises(click.exceptions.Exit) as excinfo:
        print_upload_logs([{"error": error}], "test_image")

    assert excinfo.value.exit_code == 1
    assert expected in capsys.readouterr().out


def test_error_stops_the_stream():
    """Nothing after the error is read, so the caller cannot report success."""
    consumed: list[str] = []

    def logs():
        consumed.append("first")
        yield {"error": "denied: requested access to the resource is denied"}

        consumed.append("second")
        yield {"id": "layer_a", "progress": "1/1"}

    with pytest.raises(click.exceptions.Exit):
        print_upload_logs(logs(), "test_image")

    assert consumed == ["first"]


def test_error_after_progress_moves_below_the_progress_block(
    monkeypatch: pytest.MonkeyPatch,
):
    """The cursor moves below the progress block so the error is not overwritten.

    `click.echo` strips escape sequences when the output is not a terminal, so record
    the calls instead of reading the captured output.
    """
    written: list[str] = []

    def record(message: object = "", **_kwargs: object) -> None:
        written.append(str(message))

    monkeypatch.setattr(click, "echo", record)
    monkeypatch.setattr(click, "secho", record)

    logs = [
        {"id": "layer_a", "progress": "1/2"},
        {"error": "denied: requested access to the resource is denied"},
    ]

    with pytest.raises(click.exceptions.Exit):
        print_upload_logs(logs, "test_image")

    # match the exact echo: the redraw prefix also contains `\x1b[1E`, so a substring
    # test would pass with the cursor move deleted as soon as the fixture grows
    move_below = written.index("\x1b[1E")
    error = next(i for i, m in enumerate(written) if m.startswith("denied:"))

    assert move_below < error
