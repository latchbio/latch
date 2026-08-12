from pathlib import Path
from typing import Optional

import click
import pytest

from latch_cli.services import private_images
from latch_cli.services.docker import utils as docker_utils
from latch_cli.services.private_images import resolve_workspace

ACTIVE_WS = "1111"
OTHER_WS = "2222"

WORKSPACES = {
    ACTIVE_WS: {"workspace_id": ACTIVE_WS, "name": "Active Team", "default": True},
    OTHER_WS: {"workspace_id": OTHER_WS, "name": "Other Team", "default": False},
}


def test_resolve_workspace_falls_back_to_the_active_workspace(
    monkeypatch: pytest.MonkeyPatch,
):
    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)

    assert resolve_workspace(None) == ACTIVE_WS


def test_resolve_workspace_returns_an_explicit_workspace(
    monkeypatch: pytest.MonkeyPatch,
):
    """An explicit id must win over the ambient one, or the two can disagree."""
    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)
    monkeypatch.setattr(private_images, "get_workspaces", lambda: WORKSPACES)

    assert resolve_workspace(OTHER_WS) == OTHER_WS


def test_resolve_workspace_names_the_target(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    monkeypatch.setattr(private_images, "get_workspaces", lambda: WORKSPACES)

    resolve_workspace(OTHER_WS)

    out = capsys.readouterr().out
    assert "Other Team" in out
    assert OTHER_WS in out


def test_resolve_workspace_rejects_an_unreachable_workspace(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
):
    """A typo must fail before the push, not push into someone else's namespace."""
    monkeypatch.setattr(private_images, "get_workspaces", lambda: WORKSPACES)

    with pytest.raises(click.exceptions.Exit) as excinfo:
        resolve_workspace("9999")

    assert excinfo.value.exit_code == 1
    assert "9999" in capsys.readouterr().out


def test_resolve_workspace_does_not_query_workspaces_when_implicit(
    monkeypatch: pytest.MonkeyPatch,
):
    """The default path must not pay for a lookup it does not need."""

    def unreachable() -> dict[str, object]:
        raise AssertionError("get_workspaces must not be called")

    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)
    monkeypatch.setattr(private_images, "get_workspaces", unreachable)

    assert resolve_workspace(None) == ACTIVE_WS


def test_get_credentials_uses_the_given_workspace(monkeypatch: pytest.MonkeyPatch):
    """Credentials must come from the same workspace as the push target.

    `get_credentials` used to resolve the workspace itself, so an explicit id could
    push to one workspace with credentials minted for another.
    """
    sent: dict[str, object] = {}

    class Response:
        @staticmethod
        def json() -> dict[str, str]:
            return {
                "tmp_access_key": "a",
                "tmp_secret_key": "b",
                "tmp_session_token": "c",
            }

    def post(url: str, *, headers: dict[str, str], json: dict[str, str]) -> Response:
        sent.update(json)
        return Response()

    monkeypatch.setattr(docker_utils.tinyrequests, "post", post)
    monkeypatch.setattr(docker_utils, "get_auth_header", lambda: "token")
    monkeypatch.setattr(docker_utils.boto3.session, "Session", _FakeSession)

    docker_utils.get_credentials("1111_image", ws_id=OTHER_WS)

    assert sent["ws_account_id"] == OTHER_WS


class _FakeEcr:
    @staticmethod
    def get_authorization_token() -> dict[str, list[dict[str, str]]]:
        # base64 of "AWS:password"
        return {"authorizationData": [{"authorizationToken": "QVdTOnBhc3N3b3Jk"}]}


class _FakeSession:
    def __init__(self, **kwargs: object) -> None: ...

    def client(self, name: str) -> _FakeEcr:
        return _FakeEcr()


class _Sentinel(Exception):
    """Raised from the stubbed `get_credentials` to stop `dbnp` before it builds."""


@pytest.mark.parametrize("workspace_id", [ACTIVE_WS, OTHER_WS])
def test_dbnp_forwards_the_workspace_to_get_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, workspace_id: str
):
    """The local build path mints credentials for the workspace it pushes to."""
    seen: dict[str, str] = {}

    def get_credentials(image: str, *, ws_id: str) -> object:
        seen["ws_id"] = ws_id
        raise _Sentinel

    monkeypatch.setattr(docker_utils, "get_credentials", get_credentials)

    with pytest.raises(_Sentinel):
        docker_utils.dbnp(
            None,  # type: ignore[arg-type]  # unreached: the stub raises first
            tmp_path,
            "1111_image",
            "v1",
            tmp_path / "Dockerfile",
            progress_plain=True,
            ws_id=workspace_id,
        )

    assert seen["ws_id"] == workspace_id


@pytest.mark.parametrize("remote", [True, False])
def test_build_and_upload_passes_the_workspace_to_the_build(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, remote: bool
):
    """Both build paths must be handed the resolved workspace, not resolve their own."""
    seen: dict[str, str] = {}

    def build(*_args: object, ws_id: str, **_kwargs: object) -> None:
        seen["ws_id"] = ws_id

    monkeypatch.setattr(private_images, "get_workspaces", lambda: WORKSPACES)
    monkeypatch.setattr(private_images, "remote_dbnp", build)
    monkeypatch.setattr(private_images, "dbnp", build)
    monkeypatch.setattr(private_images, "get_local_docker_client", lambda: None)
    monkeypatch.setattr(
        private_images, "record_in_db_or_exit", lambda *_args, **_kwargs: None
    )

    (tmp_path / "Dockerfile").touch()

    private_images.build_and_upload_image(
        tmp_path,
        image_name="image",
        version="v1",
        workspace_id=OTHER_WS,
        remote=remote,
        skip_confirmation=True,
    )

    assert seen["ws_id"] == OTHER_WS


@pytest.mark.parametrize("workspace_id", [None, OTHER_WS])
def test_ls_accepts_a_workspace(
    monkeypatch: pytest.MonkeyPatch, workspace_id: Optional[str]
):
    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)
    monkeypatch.setattr(private_images, "get_workspaces", lambda: WORKSPACES)

    seen: list[str] = []

    def execute(document: object, variables: dict[str, str], **_kwargs: object) -> dict:
        seen.append(variables["wsId"])
        return {"privateImages": {"nodes": []}}

    monkeypatch.setattr(private_images, "execute", execute)

    private_images.ls(workspace_id=workspace_id)

    assert seen == [workspace_id if workspace_id is not None else ACTIVE_WS]
