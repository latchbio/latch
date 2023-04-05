import asyncio
import os
from typing import Dict, Optional

import gql
import uvloop
from gql.transport.aiohttp import AIOHTTPTransport

from latch.registry.types import JSON
from latch_cli.config.latch import config


class AuthenticationError(Exception):
    ...


_transport = None


def get_transport() -> AIOHTTPTransport:
    global _transport

    if _transport is not None:
        return _transport

    # todo(ayush): make this understand sdk tokens too
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None or token == "":
        raise AuthenticationError(
            "Unable to find credentials to connect to gql server, aborting"
        )

    _transport = AIOHTTPTransport(
        url=config.gql,
        headers={"Authorization": f"Latch-Execution-Token {token}"},
    )

    return _transport


def execute(
    document: str,
    variables: Optional[Dict[str, JSON]] = None,
):
    async def helper():
        async with gql.Client(transport=get_transport()) as client:
            return await client.execute(gql.gql(document), variables)

    return asyncio.run(helper())


uvloop.install()

# todo(ayush): add generator impl for subscriptions
