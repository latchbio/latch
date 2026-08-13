"""Tests for the docker helpers behind `latch image upload` and `latch register`."""

from pathlib import Path

import docker
import pytest

from latch_cli.services.docker import utils as docker_utils

from .conftest import ACTIVE_WS, OTHER_WS


class _FakeEcr:
    @staticmethod
    def get_authorization_token() -> dict[str, list[dict[str, str]]]:
        # base64 of "AWS:password"
        return {"authorizationData": [{"authorizationToken": "QVdTOnBhc3N3b3Jk"}]}


class _FakeSession:
    def __init__(self, **kwargs: object) -> None: ...

    @staticmethod
    def client(_name: str) -> _FakeEcr:
        return _FakeEcr()


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

    def post(_url: str, *, json: dict[str, str], **_kwargs: object) -> Response:
        sent.update(json)
        return Response()

    monkeypatch.setattr(docker_utils.tinyrequests, "post", post)
    monkeypatch.setattr(docker_utils, "get_auth_header", lambda: "token")
    monkeypatch.setattr(docker_utils.boto3.session, "Session", _FakeSession)

    docker_utils.get_credentials("1111_image", ws_id=OTHER_WS)

    assert sent["ws_account_id"] == OTHER_WS


class _StubCalledError(Exception):
    """Raised from the stubbed `get_credentials` to stop `dbnp` before it builds."""


@pytest.mark.parametrize("workspace_id", [ACTIVE_WS, OTHER_WS])
def test_dbnp_forwards_the_workspace_to_get_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, workspace_id: str
):
    """The local build path mints credentials for the workspace it pushes to."""
    seen: dict[str, str] = {}

    def get_credentials(_image: str, *, ws_id: str) -> object:
        seen["ws_id"] = ws_id
        raise _StubCalledError

    monkeypatch.setattr(docker_utils, "get_credentials", get_credentials)

    with pytest.raises(_StubCalledError):
        docker_utils.dbnp(
            docker.APIClient.__new__(docker.APIClient),
            tmp_path,
            "1111_image",
            "v1",
            tmp_path / "Dockerfile",
            progress_plain=True,
            ws_id=workspace_id,
        )

    assert seen["ws_id"] == workspace_id


def test_every_build_call_site_supplies_a_workspace():
    """`ws_id` is required on the build helpers, so every caller must pass it.

    A static sweep rather than a behavioural test: `register --staging` also calls
    these, and driving it needs a real workflow package. This catches a new caller
    too, which a per-caller test would not.
    """
    import ast

    src = Path(__file__).parent.parent / "src"
    missing: list[str] = []

    for path in src.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Name):
                continue
            if node.func.id not in {"dbnp", "remote_dbnp"}:
                continue

            if not any(kw.arg == "ws_id" for kw in node.keywords):
                missing.append(f"{path.relative_to(src)}:{node.lineno}")

    assert missing == [], f"call sites missing ws_id: {missing}"
