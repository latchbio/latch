"""Utility functions for services."""

import hashlib
import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import jwt
import paramiko

from latch_cli.config.user import user_config
from latch_cli.constants import FILE_MAX_SIZE, PKG_NAME
from latch_cli.services.login import login
from latch_cli.tinyrequests import get


def retrieve_or_login() -> str:
    """Returns a valid JWT to access Latch, prompting a login flow if needed.

    Returns:
        A JWT
    """

    token = user_config.token
    if token == "":
        token = login()
    return token


def current_workspace() -> str:
    ws = user_config.workspace
    if ws == "":
        ws = account_id_from_token(retrieve_or_login())
        user_config.update_workspace(ws)
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


def generate_temporary_ssh_credentials(ssh_key_path: Path) -> str:
    # check if there is already a valid key at that path, and if so, use that
    # otherwise, if its not valid, remove it
    if ssh_key_path.exists():
        try:
            # check if file is valid + print out a fingerprint for the key
            cmd = ["ssh-keygen", "-l", "-f", ssh_key_path]
            valid_private_key = subprocess.run(cmd, check=True, capture_output=True)
            cmd = ["ssh-keygen", "-l", "-f", ssh_key_path.with_suffix(".pub")]
            valid_public_key = subprocess.run(cmd, check=True, capture_output=True)

            if valid_private_key.stdout != valid_public_key.stdout:
                raise

            # if both files are valid and their fingerprints match, use them instead of generating a new pair
            print(f"Found existing key pair at {ssh_key_path}.")
        except:
            print(f"Found malformed key-pair at {ssh_key_path}. Overwriting.")
            ssh_key_path.unlink(missing_ok=True)
            ssh_key_path.with_suffix(".pub").unlink(missing_ok=True)

    # generate private key
    if not ssh_key_path.exists():
        cmd = ["ssh-keygen", "-f", ssh_key_path, "-N", "", "-q"]
        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            raise ValueError(
                "There was a problem creating temporary SSH credentials. Please ensure"
                " that `ssh-keygen` is installed and available in your PATH."
            ) from e
        os.chmod(ssh_key_path, 0o700)

    # make key available to ssh-agent daemon
    cmd = ["ssh-add", ssh_key_path]
    try:
        subprocess.run(cmd, check=True, stderr=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        raise ValueError(
            "There was an issue adding temporary SSH credentials to your SSH Agent."
            " Please ensure that your SSH Agent is running, or (re)start it manually by"
            " running\n\n    $ eval `ssh-agent -s`\n\n"
        ) from e

    # decode private key into public key
    cmd = ["ssh-keygen", "-y", "-f", ssh_key_path]
    try:
        out = subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        raise ValueError(
            "There was a problem decoding your temporary credentials. Please ensure"
            " that `ssh-keygen` is installed and available in your PATH."
        ) from e

    public_key = out.stdout.decode("utf-8").strip("\n")
    return public_key


def get_local_package_version() -> str:
    try:
        from importlib import metadata
    except ImportError:
        import importlib_metadata as metadata
    return metadata.version(PKG_NAME)


def get_latest_package_version_request() -> str:
    cache_location = user_config.root / "cached_version"
    resp = get(f"https://pypi.org/pypi/{PKG_NAME}/json")
    version = resp.json()["info"]["version"]
    with open(cache_location, "w") as f:
        f.write(f"{version} {datetime.now().isoformat()}")
    return version


def get_latest_package_version() -> str:
    version = None
    cache_location = user_config.root / "cached_version"
    try:
        with open(cache_location, "r") as f:
            version, timestamp = f.read().split(" ")
        if datetime.now() > datetime.fromisoformat(timestamp) + timedelta(days=1):
            version = get_latest_package_version_request()
    except FileNotFoundError:
        version = get_latest_package_version_request()

    return version


class TemporarySSHCredentials:
    def __init__(self, ssh_key_path: Path):
        self._ssh_key_path = ssh_key_path
        self._public_key = None

    def generate(self):
        if self._public_key is not None:
            return
        self._public_key = generate_temporary_ssh_credentials(self._ssh_key_path)

    def cleanup(self):
        if (
            self._ssh_key_path.exists()
            and self._ssh_key_path.with_suffix(".pub").exists()
        ):
            subprocess.run(
                ["ssh-add", "-d", self._ssh_key_path],
                check=True,
                capture_output=True,
            )
        self._ssh_key_path.unlink(missing_ok=True)
        self._ssh_key_path.with_suffix(".pub").unlink(missing_ok=True)

    @property
    def public_key(self):
        self.generate()
        return self._public_key

    @property
    def private_key(self):
        self.generate()
        with open(self._ssh_key_path, "r") as f:
            return f.read()

    def __enter__(self):
        self.generate()
        return self

    def __exit__(self, type, value, traceback):
        self.cleanup()
