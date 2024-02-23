import os
import time
from typing import Any, Dict, Optional

from gql.gql import DocumentNode
from gql.transport.exceptions import TransportClosed, TransportServerError
from latch_sdk_gql import JsonValue
from latch_sdk_gql.execute import execute

from latch_cli import tinyrequests


# todo(rahul): move this function into latch_sdk_gql.execute
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
            data = execute(document, variables)
            return data
        except (TransportServerError, TransportClosed) as e:
            err = e

        if attempt < num_retries:
            # todo(rahul): tune the sleep interval based on the startup time of the vacuole
            time.sleep(2**attempt * 5)

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
