import asyncio
import base64
import json
import os
import signal
import sys
from typing import Literal, TypedDict, Union

import click
import websockets.legacy.client as websockets
from typing_extensions import TypeAlias


class StdoutResponse(TypedDict):
    stream: Literal["stdout", "stderr"]
    data: str


class CloseResponse(TypedDict):
    stream: Literal["close"]


Response: TypeAlias = Union[StdoutResponse, CloseResponse]


async def get_stdio_streams():
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader(loop=loop)

    stdin2 = os.dup(sys.stdin.fileno())
    stdout2 = os.dup(sys.stdout.fileno())

    read_transport, _ = await loop.connect_read_pipe(
        lambda: asyncio.StreamReaderProtocol(reader, loop=loop), os.fdopen(stdin2)
    )

    writer_transport, writer_protocol = await loop.connect_write_pipe(
        lambda: asyncio.streams.FlowControlMixin(loop=loop), os.fdopen(stdout2)
    )
    writer = asyncio.streams.StreamWriter(writer_transport, writer_protocol, None, loop)

    return reader, writer, read_transport


async def shutdown_on_close(
    ws: websockets.WebSocketClientProtocol, task: asyncio.Task[None]
):
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

        res = json.loads(output)
        if "error" in res:
            raise click.ClickException(f"Remote terminal failed: {res['error']}")
        if res["stream"] == "open":
            continue
        if res["stream"] == "close":
            return

        writer.write(base64.b64decode(res["data"], validate=True))
        await writer.drain()


async def handle_resize(resize_event_queue: asyncio.Queue[os.terminal_size]):
    await resize_event_queue.put(os.get_terminal_size())


async def propagate_resize_events(
    ws: websockets.WebSocketClientProtocol,
    resize_event_queue: asyncio.Queue[os.terminal_size],
):
    while True:
        cols, rows = await resize_event_queue.get()

        await ws.send(
            json.dumps({"stream": "resize", "size": {"Width": cols, "Height": rows}})
        )


async def forward_stdio(ws: websockets.WebSocketClientProtocol):
    loop = asyncio.get_event_loop()

    resize_event_queue: asyncio.Queue[os.terminal_size] = asyncio.Queue()
    await resize_event_queue.put(os.get_terminal_size())

    loop.add_signal_handler(
        signal.SIGWINCH, lambda: asyncio.create_task(handle_resize(resize_event_queue))
    )

    local_stdin, local_stdout, read_transport = await get_stdio_streams()

    workers = [
        asyncio.create_task(pipe_from_remote_stdout(ws, local_stdout)),
        asyncio.create_task(pipe_to_remote_stdin(ws, local_stdin)),
        asyncio.create_task(propagate_resize_events(ws, resize_event_queue)),
    ]
    try:
        done, _ = await asyncio.wait(workers, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        for task in workers:
            task.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        loop.remove_signal_handler(signal.SIGWINCH)
        local_stdout.close()
        read_transport.close()
