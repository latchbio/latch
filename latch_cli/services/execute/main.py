import asyncio
import base64
import json
import os
import signal
import sys
from typing import Generic, Literal, Optional, Tuple, TypedDict, TypeVar, Union
from urllib.parse import urljoin, urlparse

import websockets.client as websockets
from latch_sdk_config.latch import NUCLEUS_URL
from typing_extensions import TypeAlias

from latch_cli.services.execute.utils import (
    ContainerNode,
    EGNNode,
    ExecutionInfoNode,
    get_container_info,
    get_egn_info,
    get_execution_info,
)
from latch_cli.utils import get_auth_header


class StdoutResponse(TypedDict):
    stream: Union[Literal["stdout"], Literal["stderr"]]
    data: str


class CloseResponse(TypedDict):
    stream: Literal["close"]


Response: TypeAlias = Union[StdoutResponse, CloseResponse]


async def get_stdio_streams():
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

    return reader, writer


async def shutdown_on_close(ws: websockets.WebSocketClientProtocol, task: asyncio.Task):
    await ws.wait_closed()
    task.cancel()


async def pipe_to_remote_stdin(
    ws: websockets.WebSocketClientProtocol, reader: asyncio.StreamReader
):
    while True:
        message = await reader.read(99999)
        if message == b"":
            return

        await ws.send(
            json.dumps({
                "stream": "stdin",
                "data": base64.b64encode(message).decode("ascii"),
            })
        )


async def pipe_from_remote_stdout(
    ws: websockets.WebSocketClientProtocol, writer: asyncio.StreamWriter
):
    async for output in ws:
        if isinstance(output, bytes):
            output = output.decode()

        try:
            res = json.loads(output)
            # todo(ayush): write stderr messages to stderr lol
            if res["stream"] == "close":
                break

            writer.write(base64.b64decode(res["data"].encode("ascii")))
        except (json.JSONDecodeError, KeyError) as e:
            # todo(ayush) error surfacing
            break


async def handle_resize(resize_event_queue: asyncio.Queue):
    await resize_event_queue.put(os.get_terminal_size())


async def propagate_resize_events(
    ws: websockets.WebSocketClientProtocol,
    resize_event_queue: asyncio.Queue,
):
    while True:
        cols, rows = await resize_event_queue.get()

        await ws.send(
            json.dumps({
                "stream": "resize",
                "size": {
                    "Width": cols,
                    "Height": rows,
                },
            })
        )


async def connect(egn_info: EGNNode, container_info: Optional[ContainerNode]):
    loop = asyncio.get_event_loop()

    resize_queue: asyncio.Queue = asyncio.Queue()
    await resize_queue.put(os.get_terminal_size())

    loop.add_signal_handler(
        signal.SIGWINCH,
        lambda: asyncio.create_task(handle_resize(resize_queue)),
    )

    local_stdin, local_stdout = await get_stdio_streams()

    async with websockets.connect(
        urlparse(urljoin(NUCLEUS_URL, "/workflows/cli/shell"))
        ._replace(scheme="wss")
        .geturl(),
        close_timeout=0,
        extra_headers={"Authorization": get_auth_header()},
    ) as ws:
        request = {
            "egn_id": egn_info["id"],
            "container_index": (
                container_info["index"] if container_info is not None else None
            ),
        }

        await ws.send(json.dumps(request))

        # ayush: can't use TaskGroups bc only supported on >= 3.11
        try:
            _, pending = await asyncio.wait(
                [
                    asyncio.create_task(pipe_from_remote_stdout(ws, local_stdout)),
                    asyncio.create_task(pipe_to_remote_stdin(ws, local_stdin)),
                    asyncio.create_task(propagate_resize_events(ws, resize_queue)),
                ],
                return_when=asyncio.FIRST_COMPLETED,
            )

            for unfinished in pending:
                unfinished.cancel()

        except asyncio.CancelledError:
            pass


def exec(
    execution_id: Optional[str] = None,
    egn_id: Optional[str] = None,
    container_index: Optional[int] = None,
):
    execution_info: Optional[ExecutionInfoNode] = None
    if egn_id is None:
        execution_info = get_execution_info(execution_id)

    egn_info = get_egn_info(execution_info, egn_id)
    container_info = get_container_info(egn_info, container_index)

    import termios
    import tty

    old_settings_stdin = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin)

    try:
        asyncio.run(connect(egn_info, container_info))
    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings_stdin)
