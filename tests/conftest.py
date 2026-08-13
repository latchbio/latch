"""Fixtures shared by the private image tests."""

import docker.errors
import pytest

from latch_cli.services import private_images
from latch_cli.services.docker import utils as docker_utils

PASSWORD = "not-a-real-secret"  # noqa: S105
ACTIVE_WS = "1111"
OTHER_WS = "2222"

WORKSPACES = {
    ACTIVE_WS: {"workspace_id": ACTIVE_WS, "name": "Active Team", "default": True},
    OTHER_WS: {"workspace_id": OTHER_WS, "name": "Other Team", "default": False},
}


@pytest.fixture
def _workspaces(monkeypatch: pytest.MonkeyPatch) -> None:
    """Patch both workspace lookups. Override `current_workspace` to vary the default."""
    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)
    monkeypatch.setattr(private_images, "get_workspaces", lambda: WORKSPACES)


class _MissingImageClient:
    """A daemon with no local images, so `upload_image` reaches the pull decision."""

    def __init__(self) -> None:
        self.pulled: list[str] = []
        self.pushed: list[object] = []
        self._auth_configs: object = None

    @staticmethod
    def inspect_image(image_ref: str) -> dict[str, str]:
        raise docker.errors.ImageNotFound(image_ref)

    def pull(self, image_ref: str, **_kwargs: object) -> list[object]:
        self.pulled.append(image_ref)
        return []

    @staticmethod
    def tag(_image_ref: str, **_kwargs: object) -> bool:
        return True

    def push(self, **kwargs: object) -> list[object]:
        self.pushed.append(kwargs.get("repository"))
        return []


@pytest.fixture
def _upload_stubs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(private_images, "current_workspace", lambda: ACTIVE_WS)
    monkeypatch.setattr(private_images, "get_workspaces", lambda: WORKSPACES)
    monkeypatch.setattr(
        private_images,
        "get_credentials",
        lambda _image, **_kwargs: docker_utils.DockerCredentials(
            username="u", password=PASSWORD
        ),
    )
    monkeypatch.setattr(private_images, "print_upload_logs", lambda *_a, **_k: None)
    monkeypatch.setattr(private_images, "record_in_db_or_exit", lambda *_a, **_k: None)
