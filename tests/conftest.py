"""Fixtures shared by the private image tests."""

import pytest

from latch_cli.services import private_images

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
