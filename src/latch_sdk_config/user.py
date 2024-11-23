"""
config.user
~~~~~~~~~~~
Repository for retrieving + updating user config values.
"""

import json
from pathlib import Path
from typing import Optional


class _UserConfig:
    """User specific configuration persisted in `~/.latch/`."""

    def __init__(self):
        self._root = Path.home().resolve() / ".latch"
        self._token_path = None
        self._workspace_path = None

    @property
    def root(self):
        if not self._root.exists():
            self._root.mkdir(parents=True)
        return self._root

    @property
    def token_path(self):
        if self._token_path is None:
            self._token_path = self.root / "token"

        if not self._token_path.exists():
            self._token_path.touch()
        return self._token_path

    @property
    def workspace_path(self):
        if self._workspace_path is None:
            self._workspace_path = self.root / "workspace"

        if not self._workspace_path.exists():
            self._workspace_path.touch()
        return self._workspace_path

    @property
    def token(self) -> str:
        """The ID token used to authorize a user in interacting with Latch.

        Returns: ID token if exists else an empty string.
        """
        try:
            with open(self.token_path, "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return ""

    @property
    def workspace_id(self) -> str:
        try:
            s = self.workspace_path.read_text().strip()
            try:
                ret = json.loads(s)
                return ret["workspace_id"]
            except (json.decoder.JSONDecodeError, TypeError):
                if s == "":
                    return ""
                try:
                    int(s)
                except ValueError as e:
                    raise ValueError(
                        "Corrupted workspace configuration - please run `latch"
                        " workspace` to reset."
                    ) from e
                return s
        except FileNotFoundError:
            return ""

    @property
    def workspace_name(self) -> Optional[str]:
        try:
            s = self.workspace_path.read_text().strip()
            try:
                ret = json.loads(s)
                return ret["name"]
            except (json.decoder.JSONDecodeError, TypeError):
                return None
        except FileNotFoundError:
            return ""

    def update_token(self, token: str):
        """Updates user config with new token regardless if one exists."""
        with open(self.token_path, "w") as f:
            f.write(token)

    def update_workspace(self, workspace_id: str, name: Optional[str] = None):
        with open(self.workspace_path, "w") as f:
            f.write(json.dumps({"workspace_id": workspace_id, "name": name}))


user_config = _UserConfig()
