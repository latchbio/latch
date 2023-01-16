"""
config.user
~~~~~~~~~~~
Repository for retrieving + updating user config values.
"""

from pathlib import Path


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
        self._token_path.touch(exist_ok=True)
        return self._token_path

    @property
    def workspace_path(self):
        if self._workspace_path is None:
            self._workspace_path = self.root / "workspace"
        self._workspace_path.touch(exist_ok=True)
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
    def workspace(self) -> str:
        try:
            with open(self.workspace_path, "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return ""

    def update_token(self, token: str):
        """Updates user config with new token regardless if one exists."""
        with open(self.token_path, "w") as f:
            f.write(token)

    def update_workspace(self, workspace: str):
        with open(self.workspace_path, "w") as f:
            f.write(workspace)


user_config = _UserConfig()
