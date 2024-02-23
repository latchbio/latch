import os
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

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
    Implements exponential backoff between retries
    """
    err = None
    res = None

    attempt = 0
    while attempt < num_retries:
        attempt += 1
        try:
            assert func in req_method_map
            func = req_method_map.get(method)
            if func is None:
                return
            res = func(url, headers=headers, data=data, json=json, stream=stream)
            if res.status_code < 500:
                return res
        except ConnectionError as e:
            err = e

        time.sleep(2**attempt * 5)

    if res is None:
        raise err
    return res


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
