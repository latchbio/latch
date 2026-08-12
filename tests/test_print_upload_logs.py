import click
import pytest

from latch_cli.services.register.register import print_upload_logs


def test_progress_stream_does_not_exit():
    """A stream with no error must run to completion."""
    logs = [
        {"id": "layer_a", "progress": "1/2"},
        {"id": "layer_a", "progress": "2/2"},
        {"id": "layer_b", "progress": "1/1"},
    ]

    print_upload_logs(logs, "test_image")


def test_expired_token_error_exits_1():
    logs = [{"error": "denied: Your authorization token has expired."}]

    with pytest.raises(click.exceptions.Exit) as excinfo:
        print_upload_logs(logs, "test_image")

    assert excinfo.value.exit_code == 1


@pytest.mark.parametrize(
    "error",
    [
        "denied: requested access to the resource is denied",
        "received unexpected HTTP status: 500 Internal Server Error",
        "unauthorized: authentication required",
    ],
)
def test_any_error_exits_1(error: str):
    """Every error is fatal, not just the expired-token case."""
    with pytest.raises(click.exceptions.Exit) as excinfo:
        print_upload_logs([{"error": error}], "test_image")

    assert excinfo.value.exit_code == 1


def test_error_text_is_shown_verbatim(capsys: pytest.CaptureFixture[str]):
    error = "denied: requested access to the resource is denied"

    with pytest.raises(click.exceptions.Exit):
        print_upload_logs([{"error": error}], "test_image")

    assert error in capsys.readouterr().out


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

    def record(message: object = "", **kwargs: object) -> None:
        written.append(str(message))

    monkeypatch.setattr(click, "echo", record)
    monkeypatch.setattr(click, "secho", record)

    logs = [
        {"id": "layer_a", "progress": "1/2"},
        {"error": "denied: requested access to the resource is denied"},
    ]

    with pytest.raises(click.exceptions.Exit):
        print_upload_logs(logs, "test_image")

    # one progress line was drawn, so the cursor moves down one line before the error
    move_below = next(i for i, m in enumerate(written) if "\x1b[1E" in m)
    error = next(i for i, m in enumerate(written) if m.startswith("denied:"))

    assert move_below < error
