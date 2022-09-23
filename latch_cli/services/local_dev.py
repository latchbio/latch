import asyncio
import functools
import queue
import random
import threading
from typing import Any, Callable

import websockets

from latch_cli.utils import retrieve_or_login


async def consumer(ws, exit_signal):
    """Consumes messages from the WS and prints them to stdout"""
    async for message in ws:
        if message == exit_signal:
            return
        print(message, end="", flush=True)


async def run():
    exit_signal = str(random.getrandbits(256))

    async with websockets.connect("ws://127.0.0.1:8080/1234/ws") as ws:
        await ws.send(exit_signal)
        for _ in range(5):
            cmd = input("\x1b[38;5;8m>>> \x1b[0m")
            await ws.send(cmd)
            await consumer(ws, exit_signal)


if __name__ == "__main__":
    asyncio.run(run())
