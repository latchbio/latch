from __future__ import annotations

import asyncio
import json
import re
import subprocess  # noqa: S404
import sys
from dataclasses import dataclass
from enum import Enum
from textwrap import dedent
from typing import TYPE_CHECKING
from urllib.parse import urljoin, urlparse

import click
import gql
import websockets.client as websockets
from typing_extensions import Self

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
    # large_task = "large_task"
    small_gpu_task = "small_gpu_task"
    # large_gpu_task = "large_gpu_task"
    # v100_x1_task = "v100_x1_task"
    # v100_x4_task = "v100_x4_task"
    # v100_x8_task = "v100_x8_task"
    # g6e_xlarge_task = "g6e_xlarge_task"
    # g6e_2xlarge_task = "g6e_2xlarge_task"
    # g6e_4xlarge_task = "g6e_4xlarge_task"
    # g6e_8xlarge_task = "g6e_8xlarge_task"
    # g6e_12xlarge_task = "g6e_12xlarge_task"
    # g6e_16xlarge_task = "g6e_16xlarge_task"
    # g6e_24xlarge_task = "g6e_24xlarge_task"
    # g6e_48xlarge_task = "g6e_48xlarge_task"


human_readable_task_sizes: dict[str, InstanceSize] = {
    "Small Task": InstanceSize.small_task,
    "Medium Task": InstanceSize.medium_task,
    "Small GPU Task": InstanceSize.small_gpu_task,
}


@dataclass
class ImageInfo:
    image: str
    version: str

    __expr = re.compile(
        r"^(?:812206152185.dkr.ecr.us-west-2.amazonaws.com/)?(?P<image>[^:]+):(?P<version>[^:]+)"
    )

    @classmethod
    def from_str(cls, image_str: str) -> Self:
        match = cls.__expr.match(image_str)
        if match is None:
            raise ValueError(f"invalid image name: {image_str}")

        return cls(match["image"], match["version"])


def workflow_name(pkg_root: Path) -> str:
    name_path = pkg_root / latch_constants.pkg_workflow_name
    if not name_path.exists():
        click.secho(
            "Unable to parse workflow name - please make sure you have registered your workflow first.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    return name_path.read_text().strip()


def get_image_name(pkg_root: Path) -> str:
    ws_id = current_workspace()
    wf_name = workflow_name(pkg_root)

    prefix = ws_id
    if int(ws_id) < 10:
        prefix = f"x{ws_id}"

    suffix = wf_name
    suffix = identifier_suffix_from_str(wf_name).lower()
    suffix = docker_image_name_illegal_pat.sub("_", wf_name)

    return f"{prefix}_{suffix}"


def get_image_info(pkg_root: Path) -> ImageInfo:
    ws_id = current_workspace()

    wf_name = workflow_name(pkg_root)

    res = execute(
        gql.gql("""
        query LatestVersion($wsId: BigInt!, $name: String!) {
            workflowInfosLatestVersionInAccount(
                argOwnerId: $wsId
                filter: {name: {equalTo: $name}}
            ) {
                nodes {
                    version
                }
            }
        }
        """),
        {"wsId": ws_id, "name": wf_name},
    )["workflowInfosLatestVersionInAccount"]

    if len(res["nodes"]) != 1:
        click.secho(
            "Unable to find a registered workflow version - please make sure you have registered your workflow first.",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    latest_version = res["nodes"][0]["version"]

    return ImageInfo(get_image_name(pkg_root), latest_version)


async def rsync(pkg_root: Path, ip: str):
    ssh_command = "ssh -o CheckHostIP=no -o StrictHostKeyChecking=no -o ServerAliveInterval=30 -o ServerAliveCountMax=5 -J root@a1fd7b5d7b5824b46b8671207e1124b6-610912349.us-west-2.elb.amazonaws.com"

    while True:
        await asyncio.create_subprocess_shell(
            "rsync"
            f' --rsh="{ssh_command}"'
            " --compress"
            " --recursive"
            " --links"
            " --times"
            " --devices"
            " --specials"
            f" {pkg_root}/"
            f" root@{ip}:/root/",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        # todo(ayush): use inotify or something instead of running on a fixed interval
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

        click.secho(f"\x1b[2K\r{icon} {message}", dim=True, nl=False)

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

        q = asyncio.Queue[dict[str, str]]()
        ip, _ = await asyncio.gather(
            monitor_pod_status(conn, q), print_status_message(q)
        )

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
    image: str | None,
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
                dedent("""\
                rsync is required for latch develop. Please install rsync and try again
                    linux: apt install rsync
                    mac: brew install rsync
                """),
                fg="red",
            )
            raise click.exceptions.Exit(1) from e

    if image is not None:
        image_info = ImageInfo.from_str(image)
    elif wf_version is not None:
        image_info = ImageInfo(get_image_name(pkg_root), wf_version)
    else:
        image_info = get_image_info(pkg_root)

    click.secho("Initiating local development session", fg="blue")
    click.echo(click.style("Selected image: ", fg="blue") + image_info.image)
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
            session(
                pkg_root,
                image_info.image,
                image_info.version,
                ssh.public_key,
                size,
                disable_sync,
            )
        )
