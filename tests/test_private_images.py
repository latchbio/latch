from collections.abc import Callable
from typing import Any, Optional

import click
import graphql
import pytest

from latch_cli.services import private_images
from latch_cli.services.private_images import (
    is_recorded_in_db,
    record_failed_exit_code,
    record_in_db,
    record_in_db_or_exit,
)

WS_ID = "1234"
IMAGE_NAME = "1234_barcode_tools"
VERSION = "abc123"

RECORDED_NODE = {"workspaceId": WS_ID, "imageName": IMAGE_NAME, "version": VERSION}
OTHER_WORKSPACE_NODE = {
    "workspaceId": "9999",
    "imageName": IMAGE_NAME,
    "version": VERSION,
}


def fake_execute(
    response: Optional[dict[str, Any]], calls: list[tuple[str, Any]]
) -> Callable[..., dict[str, Any]]:
    """Return a stand-in for `execute` that records the document and its variables."""

    def execute(
        document: graphql.DocumentNode,
        variables: Optional[dict[str, Any]] = None,
        **_kwargs: object,
    ) -> dict[str, Any]:
        calls.append((graphql.print_ast(document), variables))

        return {"privateImages": response}

    return execute


OTHER_IMAGE_NODE = {
    "workspaceId": WS_ID,
    "imageName": "1234_other_tool",
    "version": VERSION,
}
OTHER_VERSION_NODE = {
    "workspaceId": WS_ID,
    "imageName": IMAGE_NAME,
    "version": "zzz999",
}


@pytest.mark.parametrize(
    ("response", "expected"),
    [
        ({"nodes": [RECORDED_NODE]}, True),
        ({"nodes": [OTHER_IMAGE_NODE]}, False),
        ({"nodes": [OTHER_VERSION_NODE]}, False),
        ({"nodes": [OTHER_WORKSPACE_NODE]}, False),
        ({"nodes": [OTHER_IMAGE_NODE, RECORDED_NODE]}, True),
        ({"nodes": []}, False),
        ({"nodes": None}, False),
        (None, False),
    ],
)
def test_is_recorded_in_db(
    monkeypatch: pytest.MonkeyPatch,
    response: Optional[dict[str, Any]],
    *,
    expected: bool,
):
    """Only a node matching both the image and the version counts as recorded."""
    monkeypatch.setattr(private_images, "execute", fake_execute(response, []))

    assert is_recorded_in_db(WS_ID, IMAGE_NAME, VERSION) is expected


def test_record_in_db_creates_the_record_despite_an_unrelated_node(
    monkeypatch: pytest.MonkeyPatch,
):
    """The create must still run when the workspace holds only other images."""
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [OTHER_IMAGE_NODE]}, calls)
    )

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 2
    assert "createPrivateImage" in calls[1][0]


def test_record_in_db_skips_the_mutation_when_already_recorded(
    monkeypatch: pytest.MonkeyPatch,
):
    """A retry of a partly completed upload must not hit the uniqueness constraint."""
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [RECORDED_NODE]}, calls)
    )

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 1
    assert "createPrivateImage" not in calls[0][0]


def test_record_in_db_creates_the_record_when_missing(monkeypatch: pytest.MonkeyPatch):
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(private_images, "execute", fake_execute({"nodes": []}, calls))

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 2
    assert "createPrivateImage" in calls[1][0]


def test_record_in_db_or_exit_is_silent_on_success(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    monkeypatch.setattr(
        private_images, "record_in_db", lambda _ws_id, _image_name, _version: None
    )

    record_in_db_or_exit(WS_ID, IMAGE_NAME, VERSION, full_image_ref="ecr/image:abc123")

    assert capsys.readouterr().out == ""


def test_record_in_db_or_exit_uses_a_distinct_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """A record failure must not look like a push failure, which exits 1."""

    def raises(_ws_id: str, _image_name: str, _version: str) -> None:
        raise RuntimeError("uniqueness violation")

    monkeypatch.setattr(private_images, "record_in_db", raises)

    full_image_ref = f"ecr/{IMAGE_NAME}:{VERSION}"

    with pytest.raises(click.exceptions.Exit) as excinfo:
        record_in_db_or_exit(WS_ID, IMAGE_NAME, VERSION, full_image_ref=full_image_ref)

    # the literal is the promise to callers; asserting the constant is self-referential
    assert excinfo.value.exit_code == 3
    assert record_failed_exit_code == 3

    out = capsys.readouterr().out
    assert full_image_ref in out
    assert "uniqueness violation" in out


def test_record_in_db_or_exit_passes_through_a_click_exit(
    monkeypatch: pytest.MonkeyPatch,
):
    """A click exit from below must keep its own code, not be relabelled as 3."""

    def raises(_ws_id: str, _image_name: str, _version: str) -> None:
        raise click.exceptions.Exit(1)

    monkeypatch.setattr(private_images, "record_in_db", raises)

    with pytest.raises(click.exceptions.Exit) as excinfo:
        record_in_db_or_exit(
            WS_ID, IMAGE_NAME, VERSION, full_image_ref="ecr/image:abc123"
        )

    assert excinfo.value.exit_code == 1


def test_record_failure_message_survives_a_multiline_error(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """A multi-line error must not defeat the dedent and skew the whole message."""

    def raises(_ws_id: str, _image_name: str, _version: str) -> None:
        raise RuntimeError("Transport error:\n{'message': 'duplicate key'}")

    monkeypatch.setattr(private_images, "record_in_db", raises)

    with pytest.raises(click.exceptions.Exit):
        record_in_db_or_exit(
            WS_ID, IMAGE_NAME, VERSION, full_image_ref="ecr/image:abc123"
        )

    out = capsys.readouterr().out
    assert out.startswith("The image reached the registry")
    # every line of the template is flush left; only the error body is indented
    assert "\n`ecr/image:abc123` is pushed" in out


def test_is_recorded_in_db_sends_all_three_variables(monkeypatch: pytest.MonkeyPatch):
    calls: list[tuple[str, Any]] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [RECORDED_NODE]}, calls)
    )

    is_recorded_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert calls[0][1] == {"wsId": WS_ID, "imageName": IMAGE_NAME, "version": VERSION}
