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
    TemporarySSHCredentials,
    current_workspace,
    retrieve_or_login,
)

config = LatchConfig()
sdk_endpoints = config.sdk_endpoints

QUIT_COMMANDS = ["quit", "exit"]


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

    with TemporarySSHCredentials(key_path) as ssh:
        cent_resp = post(
            sdk_endpoints["provision-centromere"],
            headers=headers,
            json={
                "public_key": ssh.public_key,
            },
        )

        init_local_dev_resp = post(
            sdk_endpoints["local-development"],
            headers=headers,
            json={
                "ws_account_id": current_workspace(),
                "pkg_root": pkg_root.name,
            },
        )

        if init_local_dev_resp.status_code == 403:
            raise ValueError("You are not authorized to use this feature.")

        cent_data = cent_resp.json()
        centromere_ip = cent_data["ip"]
        centromere_username = cent_data["username"]

        init_local_dev_data = init_local_dev_resp.json()
        access_key = init_local_dev_data["docker"]["tmp_access_key"]
        secret_key = init_local_dev_data["docker"]["tmp_secret_key"]
        session_token = init_local_dev_data["docker"]["tmp_session_token"]
        port = init_local_dev_data["port"]
        command = init_local_dev_data["cmd"]

        # ecr setup
        try:
            ecr_client = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name="us-west-2",
            ).client("ecr")

            dockerAccessToken = ecr_client.get_authorization_token()[
                "authorizationData"
            ][0]["authorizationToken"]
        except Exception as err:
            raise ValueError(f"unable to retrieve an ecr login token") from err

        ws_id = current_workspace()
        if int(ws_id) < 10:
            ws_id = f"x{ws_id}"

        try:
            image_name = f"{ws_id}_{pkg_root.name}"
            jmespath_expr = (
                "sort_by(imageDetails, &to_string(imagePushedAt))[-1].imageTags"
            )
            paginator = ecr_client.get_paginator("describe_images")
            iterator = paginator.paginate(repositoryName=image_name)
            filter_iterator = iterator.search(jmespath_expr)
            latest = next(filter_iterator)
            image_name_tagged = f"{config.dkr_repo}/{image_name}:{latest}"
        except:
            raise ValueError(
                "This workflow hasn't been registered yet. Please register at least once to use this feature."
            )

        # scp setup
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.connect(centromere_ip, username=centromere_username)
        scp_client = scp.SCPClient(ssh_client.get_transport(), buff_size=5 * 2**20)

        await aioconsole.aprint("Copying your local changes... ")
        # TODO(ayush) do something more sophisticated/only send over
        # diffs or smth to make this more efficient (rsync?)
        scp_client.put(
            files=pkg_root,
            remote_path=f"~/",
            recursive=True,
        )
        await aioconsole.aprint("Done.\n")

        exit_signal = str(random.getrandbits(256))
        async with websockets.connect(f"ws://{centromere_ip}:{port}/ws") as ws:
            await ws.send(exit_signal)

            await ws.send(dockerAccessToken)
            await ws.send(image_name_tagged)
            await aioconsole.aprint(
                f"Pulling {image_name}, this will only take a moment...", end="\n"
            )
            await flush_response(ws, exit_signal)
            await aioconsole.aprint("Image successfully pulled.", end="\n")

            while True:
                cmd = await aioconsole.ainput(prompt="\x1b[38;5;8m>>> \x1b[0m")
                if cmd in QUIT_COMMANDS:
                    await aioconsole.aprint("Exiting local development session")
                    break

                if cmd.startswith("run"):
                    await aioconsole.aprint("Syncing your local changes...", end="\n")
                    scp_client.put(pkg_root, f"~/", recursive=True)
                    await aioconsole.aprint("Finished. Streaming logs:", end="\n")

                await ws.send(cmd)
                await print_response(ws, exit_signal)

        close_resp = post(
            sdk_endpoints["close-local-development"],
            headers={"Authorization": f"Bearer {retrieve_or_login()}"},
            json={"command": command},
        )

        print(close_resp.status_code)


def local_development(pkg_root: Path):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_local_dev_session(pkg_root))
