import asyncio
import json
import secrets
import sys
from typing import Optional
from urllib.parse import urljoin, urlparse

import click
import websockets.client as websockets
import websockets.exceptions as ws_exceptions
from latch_sdk_config.latch import NUCLEUS_URL

from latch_cli.utils import get_auth_header

from .utils import get_pvc_info
from .ws_utils import forward_stdio


async def connect(execution_id: str, session_id: str):
    async with websockets.connect(
        urlparse(urljoin(NUCLEUS_URL, "/workflows/cli/attach-nf-workdir"))
        ._replace(scheme="wss")
        .geturl(),
        close_timeout=0,
        extra_headers={"Authorization": get_auth_header()},
    ) as ws:
        request = {"execution_id": int(execution_id), "session_id": session_id}

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


def get_session_id():
    return secrets.token_bytes(8).hex()


def attach(execution_id: Optional[str] = None):
    execution_id = get_pvc_info(execution_id)
    session_id = get_session_id()

    click.secho(
        "Attaching to workdir - this may take a few seconds...", dim=True, italic=True
    )

    import termios
    import tty

    old_settings_stdin = termios.tcgetattr(sys.stdin.fileno())
    tty.setraw(sys.stdin)

    msg = ""
    try:
        asyncio.run(connect(execution_id, session_id))
    except ws_exceptions.ConnectionClosedError as e:
        msg = json.loads(e.reason)["error"]
    except RuntimeError as e:
        msg = str(e)
    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings_stdin)

    if msg != "":
        click.secho(msg, fg="red")
