import asyncio
import difflib
import json
import os
import re
import signal
import sys
import termios
import tty
from enum import Enum
from pathlib import Path
from typing import Callable, Optional, Union

import shlex
import aioconsole
import asyncssh
import boto3
import websockets.client as websockets
import websockets.exceptions
from prompt_toolkit.patch_stdout import patch_stdout
import subprocess

from latch_cli.config.latch import config
from latch_cli.tinyrequests import post
from latch_cli.utils import (
    TemporarySSHCredentials,
    current_workspace,
    retrieve_or_login,
)


def _get_latest_image(pkg_root: Path) -> str:
    ws_id = current_workspace()
    if int(ws_id) < 10:
        ws_id = f"x{ws_id}"

    registry_name = f"{ws_id}_{pkg_root.name}"

    resp = post(
        config.api.workflow.get_latest,
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "registry_name": registry_name,
            "ws_account_id": current_workspace(),
        },
    )

    try:
        resp.raise_for_status()
        latest_version = resp.json()["version"]
    except:
        raise ValueError(
            "There was an issue getting your workflow's docker image. Please make sure"
            " you've registered your workflow at least once."
        )

    return f"{config.dkr_repo}/{ws_id}_{pkg_root.name}:{latest_version}"


async def _poll_rsync(
    pkg_root: Path,
    ip: str,
    ssh_port: str,
    key_path: Path,
    interval: float = 0.5,
):
    command = [
        "rsync",
        "-e",
        f"'ssh -p {ssh_port} -i {str(key_path)} -o StrictHostKeyChecking=no'",
        "-az",
        "--delete",
        f"{str(pkg_root)}",
        f"root@{ip}:~/{pkg_root.name}/",
    ]
    total_fails = 0
    while True:
        outs = await asyncio.subprocess.create_subprocess_shell(
            " ".join(command),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        if outs.returncode != 0:
            total_fails += 1

        if total_fails > 100:
            stdout, stderr = await outs.communicate()
            await aioconsole.aprint(stdout.decode("utf-8"))
            await aioconsole.aprint(stderr.decode("utf-8"))
            raise RuntimeError("rsync failed too many times")

        await asyncio.sleep(interval)


class _MessageType(Enum):
    resize = "resize"
    body = "body"
    exit = "exit"


async def _get_messages(
    ws: websockets.WebSocketClientProtocol,
    show_output: bool,
):
    async for message in ws:
        msg = json.loads(message)
        if msg.get("Type") == _MessageType.exit.value:
            return
        if show_output:
            await aioconsole.aprint(
                msg.get("Body"),
                end="",
                flush=True,
            )


async def _send_message(
    ws: websockets.WebSocketClientProtocol,
    message: Union[str, bytes],
    typ: Optional[_MessageType] = None,
):
    if isinstance(message, bytes):
        message = message.decode("utf-8")
    if typ is None:
        typ = _MessageType.body
    body = {"Body": message, "Type": typ.value}
    await ws.send(json.dumps(body))
    # yield control back to the event loop,
    # see https://github.com/aaugustin/websockets/issues/867
    await asyncio.sleep(0)


async def _send_resize_message(
    ws: websockets.WebSocketClientProtocol,
    term_width: int,
    term_height: int,
):
    await _send_message(
        ws,
        json.dumps(
            {
                "Width": term_width,
                "Height": term_height,
            }
        ),
        typ=_MessageType.resize,
    )


async def _shell_session(
    ws: websockets.WebSocketClientProtocol,
):
    old_settings_stdin = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin)

    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(loop=loop)

    stdin2 = os.dup(sys.stdin.fileno())
    stdout2 = os.dup(sys.stdout.fileno())

    old_sigwinch_handler = signal.getsignal(signal.SIGWINCH)

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

    resize_event_queue = asyncio.Queue()

    def new_sigwinch_handler(signum, frame):
        if isinstance(old_sigwinch_handler, Callable):
            old_sigwinch_handler(signum, frame)

        term_width, term_height = os.get_terminal_size()
        resize_event_queue.put_nowait((term_width, term_height))

    signal.signal(signal.SIGWINCH, new_sigwinch_handler)

    # get initial terminal size and send it over
    term_width, term_height = os.get_terminal_size()
    resize_event_queue.put_nowait((term_width, term_height))

    async def resize_task():
        while True:
            (term_width, term_height) = await resize_event_queue.get()
            await _send_resize_message(ws, term_width, term_height)

    async def input_task():
        while True:
            message = await reader.read(1)
            await _send_message(ws, message)

    async def output_task():
        async for output in ws:
            obj: dict = json.loads(output)
            if obj.get("Type") == "exit":
                io_task.cancel()
                return

            message = obj.get("Body")
            if isinstance(message, str):
                message = message.encode("utf-8")
            writer.write(message)
            await writer.drain()
            await asyncio.sleep(0)

    try:
        io_task = asyncio.gather(input_task(), output_task(), resize_task())
        await io_task
    except asyncio.CancelledError:
        ...
    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings_stdin)
        signal.signal(signal.SIGWINCH, old_sigwinch_handler)


async def _run_local_dev_session(pkg_root: Path):
    # hit the endpoint to make sure that a workflow image exists in ecr before
    # doing anything
    _get_latest_image(pkg_root)

    key_path = pkg_root / ".ssh_key"

    await aioconsole.aprint(str(key_path))

    with TemporarySSHCredentials(key_path) as ssh:
        headers = {"Authorization": f"Bearer {retrieve_or_login()}"}

        try:
            cent_resp = post(
                config.api.centromere.provision,
                headers=headers,
                json={"public_key": ssh.public_key},
            )
            cent_resp.raise_for_status()

            init_local_dev_resp = post(
                config.api.centromere.start_local_dev,
                headers=headers,
                json={
                    "ws_account_id": current_workspace(),
                    "pkg_root": pkg_root.name,
                },
            )
            init_local_dev_resp.raise_for_status()
        except Exception as e:
            raise ValueError(f"Unable to provision a remote instance: {e}")

        if init_local_dev_resp.status_code == 403:
            raise ValueError("You are not authorized to use this feature.")

        try:
            cent_data = cent_resp.json()
            centromere_ip = cent_data["ip"]
            centromere_username = cent_data["username"]
        except KeyError as e:
            raise ValueError(f"Malformed response from provision endpoint: missing {e}")

        try:
            init_local_dev_data = init_local_dev_resp.json()
            docker_info = init_local_dev_data["docker"]
            access_key = docker_info["tmp_access_key"]
            secret_key = docker_info["tmp_secret_key"]
            session_token = docker_info["tmp_session_token"]
            port = init_local_dev_data["port"]
            ssh_port = init_local_dev_data["ssh_port"]
        except KeyError as e:
            raise ValueError(
                f"Malformed response from initialization endpoint: missing {e}"
            )

        # ecr setup
        ws_id = current_workspace()
        if int(ws_id) < 10:
            ws_id = f"x{ws_id}"

        try:
            ecr_client = boto3.session.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
                region_name="us-west-2",
            ).client("ecr")

            ecr_auth = ecr_client.get_authorization_token()
            docker_access_token = ecr_auth["authorizationData"][0]["authorizationToken"]

        except Exception as err:
            raise ValueError(f"unable to retrieve an ecr login token") from err

        async with asyncssh.connect(
            centromere_ip, username=centromere_username
        ):
            async for ws in websockets.connect(
                f"ws://{centromere_ip}:{port}/ws",
                close_timeout=0,
                extra_headers=headers,
            ):
                try:
                    await aioconsole.aprint(
                        "Successfully connected to remote instance."
                    )
                    try:
                        await aioconsole.aprint("Setting up local sync...")
                        asyncio.create_task(_poll_rsync(pkg_root, centromere_ip, ssh_port, key_path))
                        await aioconsole.aprint("Done.")

                        await _send_message(ws, "shell")

                        image_name_tagged = _get_latest_image(pkg_root)
                        await _send_message(ws, docker_access_token)
                        await _send_message(ws, image_name_tagged)
                        await aioconsole.aprint(f"Pulling {image_name_tagged}... ")

                        await _get_messages(ws, show_output=False)
                        await _get_messages(ws, show_output=False)
                        await aioconsole.aprint("Image successfully pulled.\n")

                        await _get_messages(ws, show_output=True)

                        with patch_stdout(raw=True):
                            await _shell_session(ws)
                            await ws.close()
                    except websockets.exceptions.ConnectionClosed:
                        continue
                except (KeyboardInterrupt, EOFError):
                    await ws.close()
                except Exception as e:
                    await aioconsole.aprint(f"Error: {e}")
                    await ws.close()
                finally:
                    await aioconsole.aprint("Exiting local development session")
                    close_resp = post(
                        config.api.centromere.stop_local_dev,
                        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
                    )
                    close_resp.raise_for_status()
                    return


def local_development(pkg_root: Path):
    """Starts a REPL that allows a user to interactively run tasks to help with
    debugging during workflow development.

    In this REPL, you can run tasks or scripts and make edits to them without
    having to reregister your workflow. You can also get a shell into a
    container with the same environment as the one that the workflow runs in, to
    help debug installation issues. See the full documentation for `Local
    Development` for more info.

    Like `get_executions`, this should only be called from the CLI for best
    results.

    Args:
        pkg_root: A path that points to a valid workflow directory (see the
            docs for `register`)

    """
    asyncio.run(_run_local_dev_session(pkg_root))
