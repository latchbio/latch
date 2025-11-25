from __future__ import annotations

import asyncio
import json
import os
import shlex
import subprocess  # noqa: S404
import sys
from datetime import datetime, timezone
from enum import Enum
from typing import TYPE_CHECKING, Optional
from urllib.parse import urljoin, urlparse

import click
import dateutil.parser as dp
import gql
import websockets.client as websockets

from latch.utils import current_workspace
from latch_sdk_config.latch import NUCLEUS_URL
from latch_sdk_gql.execute import execute

from ...constants import docker_image_name_illegal_pat, latch_constants
from ...utils import (
    TemporarySSHCredentials,
    get_auth_header,
    identifier_suffix_from_str,
)
from .ws_utils import forward_stdio

if TYPE_CHECKING:
    from pathlib import Path

max_polls = 1800


# not going to allow most of these for now
class InstanceSize(str, Enum):
    small_task = "small_task"
    medium_task = "medium_task"
    large_task = "large_task"
    small_gpu_task = "small_gpu_task"
    large_gpu_task = "large_gpu_task"
    v100_x1_task = "v100_x1_task"
    # v100_x4_task = "v100_x4_task"
    # v100_x8_task = "v100_x8_task"
    g6e_xlarge_task = "g6e_xlarge_task"
    # g6e_2xlarge_task = "g6e_2xlarge_task"
    # g6e_4xlarge_task = "g6e_4xlarge_task"
    # g6e_8xlarge_task = "g6e_8xlarge_task"
    # g6e_12xlarge_task = "g6e_12xlarge_task"
    # g6e_16xlarge_task = "g6e_16xlarge_task"
    # g6e_24xlarge_task = "g6e_24xlarge_task"
    # g6e_48xlarge_task = "g6e_48xlarge_task"


def workflow_name(pkg_root: Path) -> str:
    name_path = pkg_root / latch_constants.pkg_workflow_name
    if not name_path.exists():
        click.secho(
            "Unable to parse workflow name - please make sure you have registered your workflow first.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    return name_path.read_text().strip()


def get_image(pkg_root: Path) -> str:
    ws_id = current_workspace()
    wf_name = workflow_name(pkg_root)

    prefix = ws_id
    if int(ws_id) < 10:
        prefix = f"x{ws_id}"

    suffix = wf_name
    suffix = identifier_suffix_from_str(wf_name).lower()
    suffix = docker_image_name_illegal_pat.sub("_", wf_name)

    return f"{prefix}_{suffix}"


def get_latest_registered_version(pkg_root: Path) -> str:
    ws_id = current_workspace()

    wf_name = workflow_name(pkg_root)

    res = execute(
        gql.gql("""
        query LatestVersion($wsId: BigInt!, $name: String!) {
            latchDevelopStagingImages(
                filter: {
                    ownerId: { equalTo: $wsId }
                    workflowName: { equalTo: $name }
                }
                orderBy: CREATION_TIME_DESC
                first: 1
            ) {
                nodes {
                    version
                    creationTime
                }
            }
            workflowInfosLatestVersionInAccount(
                argOwnerId: $wsId
                filter: {name: {equalTo: $name}}
            ) {
                nodes {
                    version
                    creationTime: creationDate
                }
            }
        }
        """),
        {"wsId": ws_id, "name": wf_name},
    )

    registered = res["workflowInfosLatestVersionInAccount"]["nodes"]
    staging = res["latchDevelopStagingImages"]["nodes"]

    if len(registered) == 0 and len(staging) == 0:
        click.secho(
            "Unable to find a registered workflow version - please make sure you have registered your workflow first.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    latest_creation_time = datetime.fromtimestamp(0, tz=timezone.utc)
    latest_version: Optional[str] = None

    for x in [registered, staging]:
        if len(x) == 0:
            continue

        t = dp.isoparse(x[0]["creationTime"])
        if latest_creation_time >= t:
            continue

        latest_creation_time = t
        latest_version = x[0]["version"]

    assert latest_version is not None

    return latest_version


async def rsync(pkg_root: Path, ip: str):
    ssh_command = shlex.join([
        "ssh",
        "-o",
        "CheckHostIP=no",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=5",
        "-J",
        f"root@{latch_constants.ssh_forwarder_elb_host}",
    ])

    rsync_command: list[str] = [
        "rsync",
        f'--rsh="{ssh_command}"',
        "--compress",
        "--recursive",
        "--links",
        "--times",
        "--devices",
        "--specials",
    ]

    if (pkg_root / ".gitignore").exists():
        rsync_command.extend(["--exclude-from", str(pkg_root / ".gitignore")])

    if (pkg_root / ".dockerignore").exists():
        rsync_command.extend(["--exclude-from", str(pkg_root / ".dockerignore")])

    rsync_command.extend([f"{pkg_root}/", f"root@{ip}:/root/"])

    while True:
        proc = await asyncio.create_subprocess_shell(
            " ".join(rsync_command),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=os.environ,
        )
        await proc.communicate()

        await asyncio.sleep(0.5)


async def monitor_pod_status(
    conn: websockets.WebSocketClientProtocol, queue: asyncio.Queue[dict[str, str]]
) -> str:
    while True:
        data = json.loads(await conn.recv())
        await queue.put(data)

        if data["state"] == "running":
            return data["pod_ip"]


async def print_status_message(queue: asyncio.Queue[dict[str, str]]) -> None:
    loading_icons = "—\\|/"
    message = "Creating Pod ..."
    idx = 0

    while True:
        try:
            data = queue.get_nowait()
            if data["state"] == "running":
                break

            message = data["message"]
        except asyncio.QueueEmpty:
            pass

        icon = loading_icons[idx]
        idx += 1
        idx %= 4

        click.secho(f"\x1b[2K\r{message} {icon}", dim=True, nl=False)

        await asyncio.sleep(0.25)


async def session(
    pkg_root: Path,
    image: str,
    version: str,
    ssh_pub_key: str,
    instance_size: InstanceSize,
    disable_sync: bool,
):
    async with websockets.connect(
        urlparse(urljoin(NUCLEUS_URL, "/workflows/cli/develop"))
        ._replace(scheme="wss")
        .geturl(),
        close_timeout=0,
        extra_headers={"Authorization": get_auth_header()},
    ) as conn:
        request_data = {
            "image_name": image,
            "version": version,
            "public_key": ssh_pub_key,
            "instance_size": instance_size,
        }

        await conn.send(json.dumps(request_data))
        data = json.loads(await conn.recv())

        if "error" in data:
            error_data = data["error"]
            if error_data["source"] == "unauthorized":
                click.secho(
                    "Your token is invalid - please log in using `latch login`.",
                    fg="red",
                )
                raise click.exceptions.Exit(1)

            if error_data["source"] == "forbidden":
                click.secho(
                    "You do not have adequate permissions for this workflow.", fg="red"
                )
                raise click.exceptions.Exit(1)

            click.secho(f"Unexpected error {error_data}.", fg="red")
            raise click.exceptions.Exit(1)

        _ = json.loads(await conn.recv())

        # hide cursor
        click.echo("\x1b[?25l", nl=False)

        q = asyncio.Queue[dict[str, str]]()
        ip, _ = await asyncio.gather(
            monitor_pod_status(conn, q), print_status_message(q)
        )

        # clear + show cursor
        click.echo("\x1b[2K\r\x1b[?25h", nl=False)

        import termios
        import tty

        old_settings_stdin = termios.tcgetattr(sys.stdin.fileno())
        tty.setraw(sys.stdin)

        try:
            coros = [asyncio.create_task(forward_stdio(conn))]
            if not disable_sync:
                coros.append(asyncio.create_task(rsync(pkg_root, ip)))

            _, pending = await asyncio.wait(coros, return_when=asyncio.FIRST_COMPLETED)

            for unfinished in pending:
                unfinished.cancel()
        finally:
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings_stdin)


def local_development(
    pkg_root: Path,
    *,
    size: InstanceSize,
    skip_confirm_dialog: bool,
    wf_version: str | None,
    disable_sync: bool,
):
    if not disable_sync:
        # ensure that rsync is installed
        try:
            subprocess.check_call(
                ["rsync", "--version"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError as e:
            click.secho(
                "`rsync` could not be found. Please either install `rsync` or run `latch develop` without it using `--disable-sync`.",
                fg="red",
            )
            raise click.exceptions.Exit(1) from e

    image = get_image(pkg_root)
    if wf_version is None:
        wf_version = get_latest_registered_version(pkg_root)

    click.secho("Initiating local development session", fg="blue")
    click.echo(click.style("Selected image: ", fg="blue") + image)
    click.echo(click.style("Selected version: ", fg="blue") + wf_version)
    click.echo(click.style("Selected instance size: ", fg="blue") + size)

    if skip_confirm_dialog:
        click.echo("Proceeding without confirmation because of --yes")
    elif not click.confirm("Proceed?", default=True):
        click.echo("Session cancelled.")
        return

    with TemporarySSHCredentials(pkg_root / ".latch" / "ssh_key") as ssh:
        click.echo(
            "Starting local development session. This may take a few minutes for larger/GPU-enabled"
            " instances.\n"
        )

        asyncio.run(
            session(pkg_root, image, wf_version, ssh.public_key, size, disable_sync)
        )
