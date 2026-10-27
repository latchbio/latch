import asyncio
import base64
import json
import sys
from urllib.parse import urljoin, urlparse

import websockets.legacy.client as websockets

# Initialize the SDK before CLI utilities to avoid their import cycle.
import latch  # pyright: ignore[reportUnusedImport] # noqa: F401
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import NUCLEUS_URL

from .k8s.ws_utils import get_stdio_streams

chunk_bytes = 32 * 1024
protocol_version = 2
transfer_seconds = 600
failure_exit_code = 1


def endpoint(route: str) -> str:
    return (
        urlparse(urljoin(NUCLEUS_URL, f"/workflows/cli/{route}"))
        ._replace(scheme="wss")
        .geturl()
    )


async def transfer(session_id: str, arguments: list[str]) -> int:
    async with websockets.connect(
        endpoint("develop-sync"),
        extra_headers={"Authorization": get_auth_header()},
        max_size=256 * 1024,
    ) as conn:
        await conn.send(
            json.dumps({
                "protocol": protocol_version,
                "session_id": session_id,
                "arguments": arguments,
            })
        )
        response = json.loads(await conn.recv())
        if response.get("stream") != "open":
            raise RuntimeError(
                f"Synchronization rejected: {response.get('error', 'invalid response')}"
            )
        reader, writer, read_transport = await get_stdio_streams()
        input_acknowledged = asyncio.Event()

        async def send_input() -> None:
            while True:
                chunk = await reader.read(chunk_bytes)
                if chunk == b"":
                    await conn.send(json.dumps({"stream": "eof"}))
                    return
                input_acknowledged.clear()
                await conn.send(
                    json.dumps({
                        "stream": "stdin",
                        "data": base64.b64encode(chunk).decode("ascii"),
                    })
                )
                await input_acknowledged.wait()

        async def receive_output() -> int:
            async for raw in conn:
                response = json.loads(raw)
                stream = response.get("stream")
                if stream == "stdin_ack":
                    input_acknowledged.set()
                    continue
                if stream == "close":
                    code = response.get("exit_code")
                    return (
                        code
                        if isinstance(code, int) and code >= 0
                        else failure_exit_code
                    )
                if stream not in {"stdout", "stderr"}:
                    raise RuntimeError(
                        f"Synchronization failed: {response.get('error', 'invalid frame')}"
                    )
                chunk = base64.b64decode(response["data"], validate=True)
                if stream == "stderr":
                    sys.stderr.buffer.write(chunk)
                    sys.stderr.buffer.flush()
                    continue
                writer.write(chunk)
                await writer.drain()
            raise RuntimeError("Synchronization connection closed before completion")

        sender = asyncio.create_task(send_input())
        receiver = asyncio.create_task(receive_output())
        try:
            done, _ = await asyncio.wait(
                [sender, receiver], return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                task.result()
            return await receiver
        finally:
            for task in (sender, receiver):
                task.cancel()
            await asyncio.gather(sender, receiver, return_exceptions=True)
            writer.close()
            read_transport.close()


async def main() -> int:
    if len(sys.argv) < 5 or sys.argv[2:4] != ["develop", "rsync"]:
        raise ValueError("This helper requires an rsync development receiver command")
    return await asyncio.wait_for(transfer(sys.argv[1], sys.argv[4:]), transfer_seconds)


if __name__ == "__main__":
    try:
        exit_code = asyncio.run(main())
    except Exception as error:
        print(str(error), file=sys.stderr)
        exit_code = failure_exit_code
    sys.exit(exit_code)
