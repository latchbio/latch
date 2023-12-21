import asyncio
import json
import os
import signal
import sys
import termios
import tty
from typing import Literal, Optional, Tuple, TypedDict, Union
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


async def send_stdin(
    ws: websockets.WebSocketClientProtocol, reader: asyncio.StreamReader
):
    while True:
        message = await reader.read(1)

        await ws.send(json.dumps({"stream": "stdin", "data": message.decode("utf-8")}))


async def write_stdout(
    ws: websockets.WebSocketClientProtocol, writer: asyncio.StreamWriter
):
    async for output in ws:
        if isinstance(output, bytes):
            output = output.decode("utf-8")

        try:
            res = json.loads(output)
            # todo(ayush): write stderr messages to stderr lol
            if res["stream"] == "close":
                break

            writer.write(res["data"].encode())
        except (json.JSONDecodeError, KeyError) as e:
            # todo(ayush) error surfacing
            break

    await ws.close()


async def handle_resize(resize_event_queue: asyncio.Queue[Tuple[int, int]]):
    await resize_event_queue.put(os.get_terminal_size())


async def propagate_resize_events(
    ws: websockets.WebSocketClientProtocol,
    resize_event_queue: asyncio.Queue[Tuple[int, int]],
):
    while True:
        cols, rows = await resize_event_queue.get()

        await ws.send(
            json.dumps(
                {
                    "stream": "resize",
                    "size": {
                        "Width": cols,
                        "Height": rows,
                    },
                }
            )
        )


async def connect(egn_info: EGNNode, container_info: Optional[ContainerNode]):
    loop = asyncio.get_event_loop()

    resize_event_queue: asyncio.Queue[tuple[int, int]] = asyncio.Queue()
    await resize_event_queue.put(os.get_terminal_size())

    loop.add_signal_handler(
        signal.SIGWINCH,
        lambda: asyncio.create_task(handle_resize(resize_event_queue)),
    )

    reader, writer = await get_stdio_streams()

    async with websockets.connect(
        urlparse(urljoin("http://localhost:5000", "/workflows/exec"))
        ._replace(scheme="ws")
        .geturl(),
        close_timeout=0,
        extra_headers={"Authorization": get_auth_header()},
    ) as ws:
        request = {
            "egn_id": f'{egn_info["id"]}',
            "container_index": (
                container_info["index"] if container_info is not None else None
            ),
        }

        await ws.send(json.dumps(request))

        io_task = asyncio.gather(
            send_stdin(ws, reader),
            write_stdout(ws, writer),
            propagate_resize_events(ws, resize_event_queue),
        )

        try:
            await asyncio.gather(io_task, shutdown_on_close(ws, io_task))
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

    old_settings_stdin = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin)

    try:
        asyncio.run(connect(egn_info, container_info))
    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings_stdin)
