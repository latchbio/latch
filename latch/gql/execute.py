import asyncio
import os
from contextlib import asynccontextmanager
from typing import Any, Dict, Optional

import gql
from gql.transport.aiohttp import AIOHTTPTransport

from latch_cli.config.latch import config


def get_transport() -> AIOHTTPTransport:
    # todo(ayush): make this understand sdk tokens too
    token = "f3b0a80f4aa0342d18dc"  # os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None or token == "":
        raise ValueError(
            "Unable to find credentials to connect to gql server, aborting"
        )

    headers = {"Authorization": f"Latch-Execution-Token {token}"}
    return AIOHTTPTransport(
        url=config.gql,
        headers=headers,
    )


def execute(
    document: str,
    variables: Optional[Dict[str, Any]] = None,
):
    async def helper():
        async with gql.Client(transport=get_transport()) as client:
            ret = await client.execute(gql.gql(document), variables)

        return ret

    return asyncio.run(helper())


# todo(ayush): add generator impl for subscriptions
