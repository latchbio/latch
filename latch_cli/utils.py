"""Utility functions for services."""

import hashlib
import os
from datetime import datetime, timedelta
from pathlib import Path

import jwt

from latch_cli.config.user import UserConfig
from latch_cli.constants import FILE_MAX_SIZE, PKG_NAME
from latch_cli.services.login import login
from latch_cli.tinyrequests import get

user_conf = UserConfig()


def retrieve_or_login() -> str:
    """Returns a valid JWT to access Latch, prompting a login flow if needed.

    Returns:
        A JWT
    """

    token = user_conf.token
    if token == "":
        token = login()
    return token


def current_workspace() -> str:
    ws = user_conf.current_workspace
    if ws == "":
        ws = account_id_from_token(retrieve_or_login())
    return ws


def sub_from_jwt(token: str) -> str:
    """Extract a user sub (UUID) from a JWT minted by auth0.

    Args:
        token: JWT

    Returns:
        The user sub contained within the JWT.
    """

    payload = jwt.decode(token, options={"verify_signature": False})
    try:
        sub = payload["sub"]
    except KeyError:
        raise ValueError(
            "Provided token lacks a user sub in the data payload"
            " and is not a valid token."
        )
    return sub


def account_id_from_token(token: str) -> str:
    """Exchanges a valid JWT for a Latch account ID.

    Latch account IDs are needed for any user-specific request, eg. register
    workflows or copy files to Latch.

    Args:
        token: JWT

    Returns:
        A Latch account ID (UUID).
    """

    decoded_jwt = jwt.decode(token, options={"verify_signature": False})
    try:
        return decoded_jwt.get("id")
    except KeyError as e:
        raise ValueError("Your Latch access token is malformed") from e


def _normalize_remote_path(remote_path: str):
    if remote_path.startswith("latch://"):
        remote_path = remote_path[len("latch://") :]
    if (
        not remote_path.startswith("/")
        and not remote_path.startswith("shared")
        and not remote_path.startswith("account")
    ):
        remote_path = f"/{remote_path}"

    return remote_path


def _si_number_strings(num):
    for unit in ["", "k", "M", "G", "T", "P", "E", "Z"]:
        if abs(num) < 1000:
            # `rstrip` remoes trailing zeroes
            return f"{num:3.1f}".rstrip("0").rstrip("."), unit
        num /= 1000
    return f"{num:.1f}", "Yi"


def with_si_suffix(num, suffix="B", styled=False):
    num, unit = _si_number_strings(num)

    if styled:
        import click

        return click.style(num, fg="bright_green", bold=True) + click.style(
            f"{unit}{suffix}", fg="green"
        )

    return f"{num}{unit}{suffix}"


def hash_directory(dir_path: Path) -> str:
    m = hashlib.new("sha256")
    m.update(current_workspace().encode("utf-8"))
    for containing_path, dirnames, fnames in os.walk(dir_path):
        # for repeatability guarantees
        dirnames.sort()
        fnames.sort()
        for filename in fnames:
            path = Path(containing_path).joinpath(filename)
            m.update(str(path).encode("utf-8"))
            file_size = os.path.getsize(path)
            if file_size < FILE_MAX_SIZE:
                with open(path, "rb") as f:
                    m.update(f.read())
            else:
                print(
                    "\x1b[38;5;226m"
                    f"WARNING: {path.relative_to(dir_path.resolve())} is too large "
                    f"({with_si_suffix(file_size)}) to checksum, skipping."
                    "\x1b[0m"
                )
        for dirname in dirnames:
            path = Path(containing_path).joinpath(dirname)
            m.update(str(path).encode("utf-8"))
    return m.hexdigest()


def get_local_package_version() -> str:
    try:
        from importlib import metadata
    except ImportError:
        import importlib_metadata as metadata
    return metadata.version(PKG_NAME)


def get_latest_package_version_request() -> str:
    cache_location = user_conf.root_dir / "cached_version"
    resp = get(f"https://pypi.org/pypi/{PKG_NAME}/json")
    version = resp.json()["info"]["version"]
    with open(cache_location, "w") as f:
        f.write(f"{version} {datetime.now().isoformat()}")
    return version


def get_latest_package_version() -> str:
    version = None
    cache_location = user_conf.root_dir / "cached_version"
    try:
        with open(cache_location, "r") as f:
            version, timestamp = f.read().split(" ")
        if datetime.now() > datetime.fromisoformat(timestamp) + timedelta(days=1):
            version = get_latest_package_version_request()
    except FileNotFoundError:
        version = get_latest_package_version_request()

    return version
