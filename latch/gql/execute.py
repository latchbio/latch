import asyncio
import os
from typing import Any, Dict, Optional

import gql
from gql.transport.requests import RequestsHTTPTransport

# todo(ayush): make this understand sdk tokens too
_execution_token = (  # os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    "fd9c4aed6a5a1422f804"
)
if _execution_token is None or _execution_token == "":
    raise ValueError("Unable to find credentials to connect to gql server, aborting")

_gql_endpoint_url: str = "http://localhost:5000/graphql"
_gql_transport = RequestsHTTPTransport(
    url=_gql_endpoint_url,
    headers={"Authorization": f"Bearer {_execution_token}"},
)
_gql_client = gql.Client(transport=_gql_transport)


def execute(
    document: str,
    variables: Optional[Dict[str, Any]] = None,
):
    return _gql_client.execute(gql.gql(document), variables)


# todo(ayush): add generator impl for subscriptions
