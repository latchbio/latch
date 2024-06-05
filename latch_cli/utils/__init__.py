"""Utility functions for services."""

import hashlib
import os
import re
import shutil
import stat
import subprocess
import urllib.parse
from dataclasses import asdict, is_dataclass
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import List
from urllib.parse import urljoin

import click
import jwt
from latch_sdk_config.user import user_config

from latch.utils import current_workspace
from latch_cli.click_utils import bold
from latch_cli.constants import latch_constants
from latch_cli.tinyrequests import get

# todo(ayush): need a better way to check if "latch" has been appended to urllib
if "latch" not in urllib.parse.uses_netloc:
    urllib.parse.uses_netloc.append("latch")
    urllib.parse.uses_relative.append("latch")


def urljoins(*args: str, dir: bool = False) -> str:
    """Construct a URL by appending paths

    Paths are always joined, with extra `/`s added if missing. Does not allow
    overriding basenames as opposed to normal `urljoin`. Whether the final
    path ends in a `/` is still significant and will be preserved in the output

    >>> urljoin("latch:///directory/", "another_directory")
    latch:///directory/another_directory
    >>> # No slash means "another_directory" is treated as a filename
    >>> urljoin(urljoin("latch:///directory/", "another_directory"), "file")
    latch:///directory/file
    >>> # Unintentionally overrode the filename
    >>> urljoins("latch:///directory/", "another_directory", "file")
    latch:///directory/another_directory/file
    >>> # Joined paths as expected

    Args:
        args: Paths to join
        dir: If true, ensure the output ends with a `/`
    """

    res = args[0]
    for x in args[1:]:
        if res[-1] != "/":
            res = f"{res}/"
        res = urljoin(res, x)

    if dir and res[-1] != "/":
        res = f"{res}/"

    return res


class AuthenticationError(RuntimeError): ...


def get_auth_header() -> str:
    sdk_token = user_config.token
    execution_token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")

    if sdk_token is not None and sdk_token != "":
        header = f"Latch-SDK-Token {sdk_token}"
    elif execution_token is not None:
        header = f"Latch-Execution-Token {execution_token}"
    else:
        raise AuthenticationError("Unable to find authentication credentials.")

    return header


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


def human_readable_time(t_seconds: float) -> str:
    s = t_seconds % 60
    m = (t_seconds // 60) % 60
    h = t_seconds // 60 // 60

    x: List[str] = []
    if h > 0:
        x.append(f"{int(h):d}h")
    if m > 0:
        x.append(f"{int(m):d}m")
    if s > 0:
        x.append(f"{s:.2f}s")

    return " ".join(x)


def hash_directory(dir_path: Path) -> str:
    # todo(maximsmol): store per-file hashes to show which files triggered a version change
    click.secho("Calculating workflow version based on file content hash", bold=True)
    click.secho("  Disable with --disable-auto-version/-d", italic=True, dim=True)

    m = hashlib.new("sha256")
    m.update(current_workspace().encode("utf-8"))

    ignore_file = dir_path / ".dockerignore"
    exclude: List[str] = ["/.latch", ".git"]
    try:
        with ignore_file.open("r") as f:
            click.secho("  Using .dockerignore", italic=True)

            for l in f:
                l = l.strip()

                if l == "":
                    continue
                if l[0] == "#":
                    continue

                exclude.append(l)
    except FileNotFoundError:
        ...

    from docker.utils import exclude_paths

    paths = list(exclude_paths(dir_path, exclude))
    paths.sort()

    for item in paths:
        p = Path(dir_path / item)

        p_stat = p.stat()
        if stat.S_ISDIR(p_stat.st_mode):
            m.update(p.name.encode("utf-8"))
            continue

        m.update(p.name.encode("utf-8"))

        file_size = p_stat.st_size
        if not stat.S_ISREG(p_stat.st_mode):
            click.secho(
                f"{p.relative_to(dir_path.resolve())} is not a regular file."
                " Ignoring contents",
                fg="yellow",
                bold=True,
            )
            continue

        if file_size > latch_constants.file_max_size:
            click.secho(
                f"{p.relative_to(dir_path.resolve())} is too large"
                f" ({with_si_suffix(file_size)}) to checksum. Ignoring contents",
                fg="yellow",
                bold=True,
            )
            continue

        m.update(p.read_bytes())

    return m.hexdigest()


def generate_temporary_ssh_credentials(
    ssh_key_path: Path, *, add_to_agent: bool = True
) -> str:
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
            click.secho(
                f"Found existing key pair at {ssh_key_path}.", dim=True, italic=True
            )
        except:
            click.secho(
                f"Found malformed key-pair at {ssh_key_path}. Overwriting.",
                dim=True,
                italic=True,
            )

            ssh_key_path.unlink(missing_ok=True)
            ssh_key_path.with_suffix(".pub").unlink(missing_ok=True)

    # generate private key
    if not ssh_key_path.exists():
        ssh_key_path.parent.mkdir(parents=True, exist_ok=True)
        cmd = ["ssh-keygen", "-f", ssh_key_path, "-N", "", "-q"]

        try:
            subprocess.run(cmd, check=True)
        except subprocess.CalledProcessError as e:
            click.secho(
                dedent(f"""
                    There was a problem creating temporary SSH credentials. Please ensure
                    that `{bold("ssh-keygen")}` is installed and available in your PATH.
                """.strip()),
                fg="red",
            )

            raise click.exceptions.Exit(1) from e

        os.chmod(ssh_key_path, 0o700)

    if add_to_agent:
        # make key available to ssh-agent daemon
        cmd = ["ssh-add", ssh_key_path]

        try:
            subprocess.run(cmd, check=True, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            click.secho(
                dedent(f"""
                    There was an issue adding temporary SSH credentials to your SSH Agent.
                    Please activate your SSH Agent by running

                        {bold("$ eval `ssh-agent -s`")}

                    in your terminal.
                """.strip()),
                fg="red",
            )

            raise click.exceptions.Exit(1) from e

    # decode private key into public key
    cmd = ["ssh-keygen", "-y", "-f", ssh_key_path]
    try:
        out = subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        click.secho(
            dedent(f"""
                There was a problem decoding your temporary credentials. Please ensure
                that `{bold("ssh-keygen")}` is installed and available in your PATH.
            """.strip()),
            fg="red",
        )

        raise click.exceptions.Exit(1) from e

    public_key = out.stdout.decode("utf-8").strip("\n")
    return public_key


def get_local_package_version() -> str:
    try:
        from importlib import metadata
    except ImportError:
        import importlib_metadata as metadata
    return metadata.version(latch_constants.pkg_name)


def get_latest_package_version_request() -> str:
    cache_location = user_config.root / "cached_version"
    resp = get(f"https://pypi.org/pypi/{latch_constants.pkg_name}/json")
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


class WorkflowType(Enum):
    latchbiosdk = "latchbiosdk"
    snakemake = "snakemake"
    nextflow = "nextflow"


def identifier_suffix_from_str(x: str) -> str:
    res = ""
    for c in x:
        res += c if f"_{c}".isidentifier() else "_"
    return res


def identifier_from_str(x: str) -> str:
    return (x[0] if x[0].isidentifier() else "_") + identifier_suffix_from_str(x[1:])


def get_parameter_json_value(v):
    if is_dataclass(v):
        return asdict(v)
    elif isinstance(v, Enum):
        return v.value
    elif isinstance(v, list):
        return [get_parameter_json_value(x) for x in v]
    elif isinstance(v, dict):
        return {k: get_parameter_json_value(x) for k, x in v.items()}
    else:
        return v


def check_exists_and_rename(old: Path, new: Path):
    if not new.exists():
        os.renames(old, new)
        return

    if old.is_file():
        if new.is_file():
            print(f"Warning: A file already exists at {new} and will be overwritten.")
            os.renames(old, new)
            return

        print(
            f"Warning: {old} is a file but {new} is not. Everything within {new} will"
            " be overwritten."
        )
        shutil.rmtree(new)
        os.renames(old, new)
        return

    if new.is_file():
        print(
            f"Warning: {old} is a directory but {new} is not. {new} will be"
            " overwritten."
        )
        shutil.rmtree(new)
        os.renames(old, new)
        return

    for sub in old.iterdir():
        check_exists_and_rename(sub, new / sub.name)


underscores = re.compile(r"_+")


def best_effort_display_name(x: str) -> str:
    return underscores.sub(" ", x).title().strip()
