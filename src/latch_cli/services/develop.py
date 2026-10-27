from __future__ import annotations

import asyncio
import json
import os
import shlex
import shutil
import signal
import sys
from enum import Enum
from typing import TYPE_CHECKING, Any, Optional

import click
import gql
import websockets.legacy.client as websockets
from websockets.exceptions import ConnectionClosed

from latch.utils import current_workspace
from latch_cli.constants import latch_constants
from latch_cli.utils import get_auth_header
from latch_sdk_gql.execute import execute

from .develop_sync import endpoint
from .k8s.ws_utils import forward_stdio

if TYPE_CHECKING:
    from pathlib import Path

    from latch_sdk_gql import JsonValue

protocol_version = 2
sync_interval_seconds = 0.5
process_stop_seconds = 5
startup_seconds = 600


class InstanceSize(str, Enum):
    small_task = "small_task"
    medium_task = "medium_task"
    large_task = "large_task"
    small_gpu_task = "small_gpu_task"
    large_gpu_task = "large_gpu_task"
    v100_x1_task = "v100_x1_task"
    g6e_xlarge_task = "g6e_xlarge_task"


def workflow_name(
    pkg_root: Path, *, snakemake: bool, metadata_root: Optional[Path]
) -> str:
    if snakemake is True:
        from latch.types import metadata
        from latch_cli.snakemake.utils import load_snakemake_metadata

        load_snakemake_metadata(
            pkg_root,
            pkg_root / "latch_metadata" if metadata_root is None else metadata_root,
        )
        snakemake_metadata = metadata._snakemake_metadata  # pyright: ignore[reportPrivateUsage] # noqa: SLF001
        if snakemake_metadata is None:
            raise click.ClickException("Snakemake metadata is missing.")
        return f"{snakemake_metadata.name}_jit_register"

    name_path = pkg_root / latch_constants.pkg_workflow_name
    if name_path.exists() is False:
        raise click.ClickException("Register this workflow before development.")
    return name_path.read_text().strip()


def registered_workflow(name: str, version: Optional[str]) -> dict[str, str]:
    workflow_filter: dict[str, JsonValue] = {
        "removed": {"equalTo": False},
        "ownerId": {"equalTo": current_workspace()},
        "name": {"equalTo": name},
    }
    if version is not None:
        workflow_filter["version"] = {"equalTo": version}
    response = execute(
        gql.gql("""
            query DevelopmentWorkflow($filter: WorkflowInfoFilter!) {
                workflowInfos(filter: $filter, orderBy: CREATION_DATE_DESC, first: 1) {
                    nodes { id version }
                }
            }
        """),
        {"filter": workflow_filter},
    )
    workflows = response["workflowInfos"]["nodes"]
    if len(workflows) == 0:
        raise click.ClickException(
            "No registered workflow matches this name and version. Staging images are not supported."
        )
    return workflows[0]


async def sync_once(pkg_root: Path, session_id: str) -> None:
    command = [
        "rsync",
        "--recursive",
        "--links",
        "--times",
        "--safe-links",
        "--checksum",
        "--exclude=.git/",
        "--exclude=.latch/",
        "--rsh="
        + shlex.join([
            sys.executable,
            "-m",
            "latch_cli.services.develop_sync",
            session_id,
        ]),
    ]
    for filename in (".gitignore", ".dockerignore"):
        path = pkg_root / filename
        if path.exists() is True:
            command.extend(["--exclude-from", str(path)])
    command.extend([f"{pkg_root}/", "develop:/root/"])
    process = await asyncio.create_subprocess_exec(
        *command,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        _, stderr = await process.communicate()
        if process.returncode != 0:
            raise click.ClickException(
                f"Development synchronization failed: {stderr.decode(errors='replace').strip()}"
            )
    finally:
        if process.returncode is None:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), process_stop_seconds)
            except asyncio.TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()


async def sync_changes(pkg_root: Path, session_id: str) -> None:
    while True:
        await asyncio.sleep(sync_interval_seconds)
        await sync_once(pkg_root, session_id)


async def receive_status(conn: websockets.WebSocketClientProtocol) -> dict[str, Any]:
    data = json.loads(await conn.recv())
    if "error" in data:
        raise click.ClickException(f"Development request failed: {data['error']}")
    return data


async def session(
    pkg_root: Path,
    workflow_id: str,
    instance_size: InstanceSize,
    *,
    disable_sync: bool,
    target_domain: str,
    target_region: str,
) -> None:
    async with websockets.connect(
        endpoint("develop"),
        extra_headers={"Authorization": get_auth_header()},
        close_timeout=process_stop_seconds,
    ) as conn:
        await conn.send(
            json.dumps({
                "protocol": protocol_version,
                "workspace_id": current_workspace(),
                "workflow_id": str(workflow_id),
                "instance_size": instance_size.value,
                "disable_sync": disable_sync,
                "target_domain": target_domain,
                "target_region": target_region,
            })
        )
        while True:
            data = await asyncio.wait_for(receive_status(conn), startup_seconds)
            if data.get("state") == "running":
                session_id = data["session_id"]
                break
            if data.get("state") != "starting":
                raise click.ClickException(
                    "Backend does not support development protocol 2."
                )
            click.echo(data["message"])

        data = await asyncio.wait_for(receive_status(conn), startup_seconds)
        if data.get("stream") != "open":
            raise click.ClickException("Forch terminal did not open.")
        if disable_sync is False:
            await sync_once(pkg_root, session_id)

        import termios
        import tty

        old_settings = termios.tcgetattr(sys.stdin.fileno())
        workers = []
        try:
            tty.setraw(sys.stdin)
            workers.append(asyncio.create_task(forward_stdio(conn)))
            if disable_sync is False:
                workers.append(asyncio.create_task(sync_changes(pkg_root, session_id)))
            done, _ = await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
        finally:
            for task in workers:
                task.cancel()
            await asyncio.gather(*workers, return_exceptions=True)
            termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)


def local_development(
    pkg_root: Path,
    *,
    size: InstanceSize,
    skip_confirm_dialog: bool,
    wf_version: Optional[str],
    disable_sync: bool,
    snakemake: bool = False,
    metadata_root: Optional[Path] = None,
    target_domain: str = "2",
    target_region: str = "us-west-2",
) -> None:
    if disable_sync is False and shutil.which("rsync") is None:
        raise click.ClickException("Install rsync or pass --disable-sync.")
    name = workflow_name(pkg_root, snakemake=snakemake, metadata_root=metadata_root)
    workflow = registered_workflow(name, wf_version)
    click.echo(
        f"Workflow: {name}\nVersion: {workflow['version']}\nInstance size: {size.value}"
    )
    click.echo(
        "The session expires after one hour. Disconnect cancels the session. No SDK credentials enter the container."
    )
    if (
        skip_confirm_dialog is False
        and click.confirm("Proceed?", default=True) is False
    ):
        return
    try:
        asyncio.run(
            session(
                pkg_root,
                workflow["id"],
                size,
                disable_sync=disable_sync,
                target_domain=target_domain,
                target_region=target_region,
            )
        )
    except (ConnectionClosed, asyncio.TimeoutError) as error:
        raise click.ClickException(
            "Development connection closed or timed out. Start a new session."
        ) from error
