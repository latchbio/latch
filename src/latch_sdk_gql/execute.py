import os

try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import time
from typing import Dict, Optional

import gql
from gql.transport.requests import RequestsHTTPTransport
from graphql import DocumentNode

from latch_sdk_config.latch import config
from latch_sdk_config.user import user_config
from latch_sdk_gql import AuthenticationError, JsonValue


@cache
def _get_client() -> gql.Client:
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
            url=config.gql, headers={"Authorization": auth_header}
        )
    )


def execute(
    document: DocumentNode,
    variables: Optional[Dict[str, JsonValue]] = None,
    *,
    max_retries: int = 5,
    backoff: float = 0.1,
):
    client = _get_client()
    retries = 0
    while True:
        try:
            return client.execute(document, variables)
        except Exception:
            if retries >= max_retries:
                raise

            time.sleep(backoff * 2**retries)
            retries += 1


# todo(ayush): add generator impl for subscriptions
