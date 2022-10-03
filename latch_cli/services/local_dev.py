import asyncio
import functools
import random
from pathlib import Path

import aioconsole
import boto3
import paramiko
import scp
import websockets

from latch_cli.config.latch import LatchConfig
from latch_cli.tinyrequests import post
from latch_cli.utils import (
    current_workspace,
    generate_temporary_ssh_credentials,
    retrieve_or_login,
)

sdk_endpoints = LatchConfig().sdk_endpoints


async def print_response(ws, exit_signal):
    """Consumes messages from the WS and prints them to stdout"""
    async for message in ws:
        if message == exit_signal:
            return
        await aioconsole.aprint(message, end="", flush=True)


async def flush_response(ws, exit_signal):
    """Consumes messages from the WS and does not print them"""

    # TODO(ayush) pretty print this for docker logs
    async for message in ws:
        if message == exit_signal:
            return


async def run_local_dev_session(pkg_root: Path):
    headers = {"Authorization": f"Bearer {retrieve_or_login()}"}

    key_path = pkg_root / ".ssh_key"

    cent_resp = post(
        sdk_endpoints["provision-centromere"],
        headers=headers,
        json={
            "public_key": generate_temporary_ssh_credentials(key_path),
        },
    )

    resp = post(
        sdk_endpoints["local-development"],
        headers=headers,
        json={"ws_account_id": current_workspace()},
    )

    cent_data = cent_resp.json()
    centromere_ip = cent_data["ip"]
    centromere_username = cent_data["username"]

    resp_data = resp.json()
    access_key = resp_data["tmp_access_key"]
    secret_key = resp_data["tmp_secret_key"]
    session_token = resp_data["tmp_session_token"]

    # scp setup
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()
    ssh_client.connect(centromere_ip, username=centromere_username)
    scp_client = scp.SCPClient(ssh_client.get_transport())

    await aioconsole.aprint("Copying your local changes... ")
    # TODO(ayush) do something more sophisticated/only send over
    # diffs or smth to make this more efficient
    scp_client.put(
        files=pkg_root,
        remote_path=f"~/workflow",
        recursive=True,
    )
    await aioconsole.aprint("Done.\n")

    # ecr setup
    try:
        client = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            aws_session_token=session_token,
            region_name="us-west-2",
        ).client("ecr")

        dockerAccessToken = client.get_authorization_token()["authorizationData"][0][
            "authorizationToken"
        ]
    except Exception as err:
        raise ValueError(f"unable to retreive a login token") from err

    ws_id = current_workspace()
    if int(ws_id) < 10:
        ws_id = f"x{ws_id}"

    image_name = f"{ws_id}_{pkg_root.name}"

    jmespath_expression = (
        "sort_by(imageDetails, &to_string(imagePushedAt))[-1].imageTags"
    )
    paginator = client.get_paginator("describe_images")
    iterator = paginator.paginate(repositoryName=image_name)
    filter_iterator = iterator.search(jmespath_expression)
    latest = next(filter_iterator)
    image_name_tagged = (
        f"812206152185.dkr.ecr.us-west-2.amazonaws.com/{image_name}:{latest}"
    )

    exit_signal = str(random.getrandbits(256))
    async with websockets.connect(f"ws://{centromere_ip}:8080/ws") as ws:
        await ws.send(exit_signal)

        await ws.send(dockerAccessToken)
        await ws.send(image_name_tagged)
        await aioconsole.aprint(
            f"Pulling {image_name}, this will only take a moment...", end="\n"
        )
        await flush_response(ws, exit_signal)
        await aioconsole.aprint("Image successfully pulled.", end="\n")

        for _ in range(5):
            cmd = await aioconsole.ainput(prompt="\x1b[38;5;8m>>> \x1b[0m")
            await ws.send(cmd)

            # scp_client.put(pkg_root, f"~/workflow")
            await print_response(ws, exit_signal)

        return


def local_development(pkg_root: Path):
    asyncio.run(run_local_dev_session(pkg_root))
