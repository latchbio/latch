"""
config.user
~~~~~~~~~~~
Repository for retrieving + updating user config values.
"""

from pathlib import Path


class UserConfig:
    """User specific configuration persisted in `~/.latch/`."""

    _root = None

    def __init__(self):
        self._root = Path.home().resolve().joinpath(".latch")

    @property
    def root_dir(self):
        if self._root.exists() is False:
            self._root.mkdir(parents=True)
        return self._root

    def token_exists(self) -> bool:
        if self.token != "":
            return False
        return True

    @property
    def token(self) -> str:
        """The ID token used to authorize a user in interacting with Latch.

        Returns: ID token if exists else an empty string.
        """
        try:
            with open(self.root_dir.joinpath("token"), "r") as f:
                return f.read().strip()
        except FileNotFoundError:
            return ""

    def update_token(self, token: str):
        """Updates user config with new token regardless if one exists."""
        with open(self.root_dir.joinpath("token"), "w") as f:
            f.write(token)
