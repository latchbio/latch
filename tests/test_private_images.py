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

RECORDED_NODE = {
    "imageName": IMAGE_NAME,
    "version": VERSION,
    "creationTime": "2026-08-12T00:00:00Z",
}


def fake_execute(response: Optional[dict[str, Any]], calls: list[str]):
    """Return a stand-in for `execute` that records the operation of each call."""

    def execute(document: graphql.DocumentNode, variables=None, **kwargs):
        calls.append(graphql.print_ast(document))

        return {"privateImages": response}

    return execute


def test_is_recorded_in_db_finds_the_image(monkeypatch: pytest.MonkeyPatch):
    calls: list[str] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [RECORDED_NODE]}, calls)
    )

    assert is_recorded_in_db(WS_ID, IMAGE_NAME, VERSION) is True


def test_is_recorded_in_db_filters_on_all_three_fields(
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[str] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [RECORDED_NODE]}, calls)
    )

    is_recorded_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 1
    for field in ["workspaceId", "imageName", "version"]:
        assert field in calls[0]


@pytest.mark.parametrize("response", [{"nodes": []}, {"nodes": None}, None])
def test_is_recorded_in_db_reports_a_missing_image(
    monkeypatch: pytest.MonkeyPatch, response: Optional[dict[str, Any]]
):
    calls: list[str] = []
    monkeypatch.setattr(private_images, "execute", fake_execute(response, calls))

    assert is_recorded_in_db(WS_ID, IMAGE_NAME, VERSION) is False


def test_record_in_db_skips_the_mutation_when_already_recorded(
    monkeypatch: pytest.MonkeyPatch,
):
    """A retry of a partly completed upload must not hit the uniqueness constraint."""
    calls: list[str] = []
    monkeypatch.setattr(
        private_images, "execute", fake_execute({"nodes": [RECORDED_NODE]}, calls)
    )

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 1
    assert "createPrivateImage" not in calls[0]


def test_record_in_db_creates_the_record_when_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    calls: list[str] = []
    monkeypatch.setattr(private_images, "execute", fake_execute({"nodes": []}, calls))

    record_in_db(WS_ID, IMAGE_NAME, VERSION)

    assert len(calls) == 2
    assert "createPrivateImage" in calls[1]


def test_record_in_db_or_exit_is_silent_on_success(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(
        private_images, "record_in_db", lambda ws_id, image_name, version: None
    )

    record_in_db_or_exit(WS_ID, IMAGE_NAME, VERSION, "ecr/image:abc123")


def test_record_in_db_or_exit_uses_a_distinct_exit_code(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """A record failure must not look like a push failure, which exits 1."""

    def raises(ws_id: str, image_name: str, version: str) -> None:
        raise RuntimeError("uniqueness violation")

    monkeypatch.setattr(private_images, "record_in_db", raises)

    full_image_ref = f"ecr/{IMAGE_NAME}:{VERSION}"

    with pytest.raises(click.exceptions.Exit) as excinfo:
        record_in_db_or_exit(WS_ID, IMAGE_NAME, VERSION, full_image_ref)

    assert excinfo.value.exit_code == record_failed_exit_code
    assert excinfo.value.exit_code != 1

    out = capsys.readouterr().out
    assert full_image_ref in out
    assert "uniqueness violation" in out
