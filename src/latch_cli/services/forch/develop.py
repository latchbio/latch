import asyncio
import subprocess
from pathlib import Path
from typing import Optional, TypedDict
from urllib.parse import urljoin

import click
import gql

from latch_cli import tinyrequests
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import NUCLEUS_URL
from latch_sdk_gql.execute import execute_async

from ..k8s.develop import InstanceSize, print_status_message, rsync


class Ip(TypedDict):
    ip: str


class TaskEvent(TypedDict):
    taskEventContainerCreatedDatumById: Ip


class TaskEvents(TypedDict):
    nodes: list[TaskEvent]


class TaskIp(TypedDict):
    id: str
    taskEvents: TaskEvents


async def get_task_ip(forch_task_id: int):
    ip_res: Optional[TaskIp] = (
        await execute_async(
            gql.gql("""
                query GetTaskIp($taskId: BigInt!) {
                    task(id: $taskId) {
                        id
                        taskEvents(
                            filter: {type: {equalTo: "container-created"}}
                            orderBy: TIME_DESC
                            first: 1
                        ) {
                            nodes {
                                taskEventContainerCreatedDatumById {
                                    ip
                                }
                            }
                        }
                    }
                }
            """),
            {"taskId": str(forch_task_id)},
        )
    )["task"]

    if ip_res is None or len(ip_res["taskEvents"]["nodes"]) == 0:
        click.secho("Error creating develop container", fg="red")
        raise click.exceptions.Exit(1)

    return ip_res["taskEvents"]["nodes"][0]["taskEventContainerCreatedDatumById"]["ip"]


class Task(TypedDict):
    id: str
    status: Optional[str]


async def poll_task_status(
    forch_task_id: int, message_queue: asyncio.Queue[dict[str, str]]
):
    while True:
        task_res: Optional[Task] = (
            await execute_async(
                gql.gql("""
                    query GetTaskStatus($taskId: BigInt!) {
                        task(id: $taskId) {
                            id
                            status
                        }
                    }
                """),
                {"taskId": str(forch_task_id)},
            )
        )["task"]

        if task_res is None:
            click.secho("Unable to retrieve develop container status", fg="red")
            raise click.exceptions.Exit(1)

        status = (
            task_res["status"].lower() if task_res["status"] is not None else "pending"
        )

        # container is running but ssh is not necessarily ready yet, move on to polling that
        if status == "running":
            break

        message = "Waiting for resources"
        if status == "initializing":
            message = "Creating container"

        await message_queue.put({"state": status, "message": message})

        await asyncio.sleep(1)

    while True:
        ip = await get_task_ip(forch_task_id)

        proc = await asyncio.create_subprocess_exec(
            "ssh",
            "-o",
            "StrictHostKeyChecking=off",
            "-o",
            "ServerAliveInterval=30",
            "-o",
            "ServerAliveCountMax=5",
            "-J",
            # todo(ayush): query forwarder ip based on region
            "root@44.237.115.144",
            f"root@{ip}",
            "/bin/true",
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        await proc.communicate()
        if proc.returncode == 0:
            await message_queue.put({"state": "running"})
            return

        await message_queue.put({
            "state": "initializing",
            "message": "Waiting for SSH to become ready",
        })

        await asyncio.sleep(1)


async def session(pkg_root: Path, forch_task_id: int, disable_sync: bool):
    message_queue: asyncio.Queue[dict[str, str]] = asyncio.Queue()

    await asyncio.gather(
        poll_task_status(forch_task_id, message_queue),
        print_status_message(message_queue),
    )

    ip = await get_task_ip(forch_task_id)
    # todo(ayush): query forwarder ip based on region
    forwarder = "44.237.115.144"

    proc = await asyncio.create_subprocess_exec(
        "ssh",
        "-o",
        "StrictHostKeyChecking=off",
        "-o",
        "ServerAliveInterval=30",
        "-o",
        "ServerAliveCountMax=5",
        "-J",
        f"root@{forwarder}",
        f"root@{ip}",
    )

    if disable_sync:
        await proc.wait()
        return

    await asyncio.gather(proc.wait(), rsync(pkg_root, ip, forwarder=forwarder))


def forch_develop(
    pkg_root: Path,
    image: str,
    version: str,
    ssh_pub_key: str,
    instance_size: InstanceSize,
    disable_sync: bool,
):
    resp = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/workflows/cli/forch-develop"),
        headers={"Authorization": get_auth_header()},
        json={
            "image_name": image,
            "version": version,
            "public_key": ssh_pub_key,
            "instance_size": instance_size,
        },
    )

    if resp.status_code != 200:
        click.secho(
            f"Unable to start develop session: {resp.content.decode()}", fg="red"
        )
        raise click.exceptions.Exit(1)

    forch_task_id = resp.json().get("forch_task_id")
    if not isinstance(forch_task_id, int):
        click.secho(f"Invalid response: {resp.content.decode()}", fg="red")
        raise click.exceptions.Exit(1)

    return asyncio.run(session(pkg_root, forch_task_id, disable_sync))
