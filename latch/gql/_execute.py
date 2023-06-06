import os

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

from typing import Dict, Optional

import gql
from gql.transport.requests import RequestsHTTPTransport
from graphql import DocumentNode

from latch.gql import AuthenticationError
from latch.types.json import JsonValue
from latch_cli.config.latch import config
from latch_cli.config.user import user_config


@cache
def _get_client():
    auth_header: Optional[str] = None

    if auth_header is None:
        token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID", "")
        if token != "":
            auth_header = f"Latch-Execution-Token {token}"

    if auth_header is None:
        token = user_config.token
        if token != "":
            auth_header = f"Latch-SDK-Token {token}"

    if auth_header is None:
        raise AuthenticationError(
            "Unable to find credentials to connect to gql server, aborting"
        )

    return gql.Client(
        transport=RequestsHTTPTransport(
            url=config.gql,
            headers={"Authorization": auth_header},
        )
    )


def execute(
    document: DocumentNode,
    variables: Optional[Dict[str, JsonValue]] = None,
):
    client = _get_client()
    return client.execute(document, variables)


# todo(ayush): add generator impl for subscriptions
