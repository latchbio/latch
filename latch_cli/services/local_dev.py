import asyncio
import contextlib
import io
import json
import logging
import os
import random
import sys
import termios
import tty
from pathlib import Path
from typing import AsyncIterator, Tuple, Union

import aioconsole

# import asyncssh
import boto3
import paramiko
import scp
import websockets.client as websockets
import websockets.exceptions
from prompt_toolkit.auto_suggest import AutoSuggestFromHistory
from prompt_toolkit.completion import NestedCompleter, PathCompleter
from prompt_toolkit.eventloop.inputhook import set_eventloop_with_inputhook
from prompt_toolkit.history import FileHistory
from prompt_toolkit.patch_stdout import patch_stdout
from prompt_toolkit.shortcuts import PromptSession

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
EXEC_COMMANDS = ["shell", "run", "run-script"]
LIST_COMMANDS = ["ls", "list-tasks"]

COMMANDS = QUIT_COMMANDS + EXEC_COMMANDS + LIST_COMMANDS


async def copy_files(scp_client: scp.SCPClient, pkg_root: Path):
    if pkg_root.joinpath("wf").exists():
        scp_client.put(
            files=pkg_root.joinpath("wf"),
            remote_path=f"~/{pkg_root.name}",
            recursive=True,
        )
    else:
        await aioconsole.aprint(f"Could not find {pkg_root.joinpath('wf')} - skipping")
    if pkg_root.joinpath("data").exists():
        scp_client.put(
            files=pkg_root.joinpath("data"),
            remote_path=f"~/{pkg_root.name}",
            recursive=True,
        )
    else:
        await aioconsole.aprint(
            f"Could not find {pkg_root.joinpath('data')} - skipping"
        )
    if pkg_root.joinpath("scripts").exists():
        scp_client.put(
            files=pkg_root.joinpath("scripts"),
            remote_path=f"~/{pkg_root.name}",
            recursive=True,
        )
    else:
        await aioconsole.aprint(
            f"Could not find {pkg_root.joinpath('scripts')} - skipping"
        )


async def get_message(ws: websockets.WebSocketClientProtocol, show_output: bool):
    """Consumes messages from the WS and prints them to stdout"""
    async for message in ws:
        msg = json.loads(message)
        if msg.get("Type") == "exit":
            return
        if show_output:
            await aioconsole.aprint(
                msg.get("Body").encode("utf-8"),
                end="",
                flush=True,
            )


async def send_message(
    ws: websockets.WebSocketClientProtocol,
    message: Union[str, bytes],
):
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    body = {"Body": message, "Type": None}
    await ws.send(json.dumps(body))
    # yield control back to the event loop,
    # see https://github.com/aaugustin/websockets/issues/867
    await asyncio.sleep(0)


async def run_local_dev_session(pkg_root: Path):
    scripts_dir = pkg_root.joinpath("scripts").resolve()

    def file_filter(filename: str) -> bool:
        file_path = Path(filename).resolve()
        return file_path == scripts_dir or scripts_dir in file_path.parents

    completer = NestedCompleter(
        {
            "run-script": PathCompleter(
                get_paths=lambda: [str(pkg_root)],
                file_filter=file_filter,
            ),
            "run": None,
            "ls": None,
            "list-tasks": None,
        },
        ignore_case=True,
    )
    session = PromptSession(
        ">>> ",
        completer=completer,
        complete_while_typing=True,
        auto_suggest=AutoSuggestFromHistory(),
        history=FileHistory(pkg_root.joinpath(".latch_history")),
    )

    key_path = pkg_root / ".ssh_key"

    with TemporarySSHCredentials(key_path) as ssh:
        headers = {"Authorization": f"Bearer {retrieve_or_login()}"}

        cent_resp = post(
            sdk_endpoints["provision-centromere"],
            headers=headers,
            json={"public_key": ssh.public_key},
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

        # ecr setup
        try:
            ecr_client = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name="us-west-2",
            ).client("ecr")

            docker_access_token = ecr_client.get_authorization_token()[
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
                "This workflow hasn't been registered yet. "
                "Please register at least once to use this feature."
            )

        # scp setup
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        ssh_client.connect(centromere_ip, username=centromere_username)
        scp_client = scp.SCPClient(ssh_client.get_transport(), buff_size=5 * 2**20)

        await aioconsole.aprint("Copying your local changes... ")
        # TODO(ayush) do something more sophisticated/only send over
        # diffs or smth to make this more efficient (rsync?)
        await copy_files(scp_client, pkg_root)
        await aioconsole.aprint("Done.\n")

        try:
            async for ws in websockets.connect(
                f"ws://{centromere_ip}:{port}/ws",
                close_timeout=0,
            ):
                try:
                    await send_message(ws, docker_access_token)
                    await send_message(ws, image_name_tagged)

                    await aioconsole.aprint(
                        f"Pulling {image_name}, this will only take a moment... "
                    )
                    await get_message(ws, show_output=False)
                    await get_message(ws, show_output=False)
                    await aioconsole.aprint("Image successfully pulled.")

                    while True:
                        cmd: str = await session.prompt_async()

                        if cmd in QUIT_COMMANDS:
                            await aioconsole.aprint("Exiting local development session")
                            return

                        if cmd.startswith("run"):
                            await aioconsole.aprint("Syncing your local changes... ")
                            await copy_files(scp_client, pkg_root)
                            await aioconsole.aprint(
                                "Finished syncing. Beginning execution and streaming logs:"
                            )

                        await send_message(ws, cmd)
                        await get_message(ws, show_output=True)

                        if cmd == "shell":
                            with session.input.detach():
                                with patch_stdout(raw=True):
                                    await shell_session(ws)
                                    await ws.drain()
                except websockets.exceptions.ConnectionClosed:
                    continue
        finally:
            close_resp = post(
                sdk_endpoints["close-local-development"],
                headers={"Authorization": f"Bearer {retrieve_or_login()}"},
            )

        close_resp.raise_for_status()


async def shell_session(
    ws: websockets.WebSocketClientProtocol,
):
    old_settings_stdin = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin)

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(loop=loop)

    stdin2 = os.dup(sys.stdin.fileno())
    stdout2 = os.dup(sys.stdout.fileno())

    await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(
            reader,
            loop=loop,
        ),
        os.fdopen(stdin2),
    )

    writer_transport, writer_protocol = await loop.connect_write_pipe(
        lambda: asyncio.streams.FlowControlMixin(loop=loop),
        os.fdopen(stdout2),
    )
    writer = asyncio.streams.StreamWriter(
        writer_transport,
        writer_protocol,
        None,
        loop,
    )

    async def input_task():
        while True:
            message = await reader.read(1)
            await send_message(ws, message)

    async def output_task():
        async for output in ws:
            msg = json.loads(output)
            if msg.get("Type") == "exit":
                io_task.cancel()
                return

            message = msg.get("Body")
            if isinstance(message, str):
                message = message.encode("utf-8")
            writer.write(message)
            await writer.drain()
            await asyncio.sleep(0)

    try:
        io_task = asyncio.gather(input_task(), output_task())
        await io_task
    except asyncio.CancelledError:
        ...
    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings_stdin)


def local_development(pkg_root: Path):
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run_local_dev_session(pkg_root))
