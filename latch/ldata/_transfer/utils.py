import os
import time
from enum import Enum
from typing import Any, Callable, Dict, Optional

from gql.gql import DocumentNode
from gql.transport.exceptions import TransportClosed, TransportServerError
from latch_sdk_gql import JsonValue
from latch_sdk_gql.execute import execute

from latch_cli import tinyrequests


class HTTPMethod(Enum):
    post = "post"
    put = "put"
    get = "get"


req_method_map: Dict[HTTPMethod, Callable] = {
    HTTPMethod.get: tinyrequests.get,
    HTTPMethod.post: tinyrequests.post,
    HTTPMethod.put: tinyrequests.put,
}


def request_with_retry(
    method: HTTPMethod,
    url: str,
    *,
    headers: Optional[Dict[str, str]] = {},
    data: Optional[Any] = None,
    json: Optional[bytes] = None,
    stream: bool = False,
    num_retries: int = 3,
) -> tinyrequests.TinyResponse:
    """
    Send HTTP request. Retry on 500s or ConnectionErrors.
    Implements exponential backoff between retries.
    """
    err = None
    res = None

    attempt = 0
    while attempt < num_retries:
        res = None
        attempt += 1
        try:
            assert method in req_method_map
            func = req_method_map.get(method)
            res = func(url, headers=headers, data=data, json=json, stream=stream)
            if res.status_code < 500:
                return res
        except ConnectionError as e:
            err = e

        time.sleep(2**attempt * 3)

    if res is None:
        raise err
    return res


def query_with_retry(
    document: DocumentNode,
    variables: Optional[Dict[str, JsonValue]] = None,
    *,
    num_retries: int = 3,
) -> Dict[str, Any]:
    """
    Send GraphQL query request. Retry on Server or Connection failures.
    Implements exponential backoff between retries
    """
    attempt = 0
    while attempt < num_retries:
        attempt += 1
        try:
            data = execute(
                document,
                variables,
            )
            return data
        except (TransportServerError, TransportClosed) as e:
            err = e

        time.sleep(2**attempt * 3)

    raise err


def get_max_workers() -> int:
    try:
        max_workers = len(os.sched_getaffinity(0)) * 4
    except AttributeError:
        cpu = os.cpu_count()
        if cpu is not None:
            max_workers = cpu * 4
        else:
            max_workers = 16

    return min(max_workers, 16)
