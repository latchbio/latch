import asyncio
import json
import sys
from typing import Optional
from urllib.parse import urljoin, urlparse

import websockets.client as websockets

from latch_cli.services.k8s.utils import (
    ContainerNode,
    EGNNode,
    ExecutionInfoNode,
    get_container_info,
    get_egn_info,
    get_execution_info,
)
from latch_cli.utils import get_auth_header
from latch_sdk_config.latch import NUCLEUS_URL

from .ws_utils import forward_stdio


async def connect(egn_info: EGNNode, container_info: Optional[ContainerNode]):
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
        data = await ws.recv()

        msg = ""
        try:
            res = json.loads(data)
            if "error" in res:
                raise RuntimeError(res["error"])
        except json.JSONDecodeError:
            msg = "Unable to connect to pod - internal error."
        except RuntimeError as e:
            msg = str(e)

        if msg != "":
            raise RuntimeError(msg)

        await forward_stdio(ws)


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
