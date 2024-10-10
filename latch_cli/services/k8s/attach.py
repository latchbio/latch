import asyncio
import json
import secrets
import sys
from typing import Optional
from urllib.parse import urljoin, urlparse

import websockets.client as websockets
from latch_sdk_config.latch import NUCLEUS_URL

from latch_cli.services.k8s.utils import get_execution_info
from latch_cli.utils import get_auth_header

from .ws_utils import forward_stdio


async def connect(execution_id: str, session_id: str):
    async with websockets.connect(
        urlparse(urljoin(NUCLEUS_URL, "/workflows/cli/attach"))
        ._replace(scheme="wss")
        .geturl(),
        close_timeout=0,
        extra_headers={"Authorization": get_auth_header()},
    ) as ws:
        request = {"execution_id": execution_id, "session_id": session_id}

        await ws.send(json.dumps(request))
        await forward_stdio(ws)


def get_session_id():
    return secrets.token_bytes(18).hex()


def attach(execution_id: Optional[str] = None):
    execution_info = get_execution_info(execution_id, nextflow_only=True)
    session_id = get_session_id()

    import termios
    import tty

    old_settings_stdin = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin)

    try:
        asyncio.run(connect(execution_info["id"], session_id))
    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings_stdin)
