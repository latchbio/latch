import os
from typing import Any, Dict

import requests

NUCLEUS_URL = os.environ.get("LATCH_CLI_NUCLEUS_URL", "https://nucleus.latch.bio")
ADD_MESSAGE_ENDPOINT = f"{NUCLEUS_URL}/sdk/add-task-execution-message"


def message(typ: str, data: Dict[str, Any]) -> None:
    """Display a message prominently on the Latch console during and after a
    task execution.

    The Latch platform first processes this message internally, then displays it
    under your task's execution page.

    Args:
        typ:
            A message type that determines how your message is displayed.
            Currently one of 'info', 'warning', or 'error'.
        data:
            The data displayed on the Latch console, formatted as follows:
            ```{'title': ..., 'body': ...}```.

    Raises:
        RuntimeError: If an internal error occurs while processing the message.

    Example usage: ::

        @small_task
        def task():

            ...

            try:
                ...
            catch ValueError:
                title = 'Invalid sample ID column selected'
                body = 'Your file indicates that sample columns a, b are valid'
                message(type='error', data={'title': title, 'body': body})

            ...
    """
    task_project = os.environ.get("FLYTE_INTERNAL_TASK_PROJECT")
    task_domain = os.environ.get("FLYTE_INTERNAL_TASK_DOMAIN")
    task_name = os.environ.get("FLYTE_INTERNAL_TASK_NAME")
    task_version = os.environ.get("FLYTE_INTERNAL_TASK_VERSION")
    task_attempt_number = os.environ.get("FLYTE_ATTEMPT_NUMBER")
    execution_token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    array_index = os.environ.get("FLYTE_K8S_ARRAY_INDEX")

    if task_project is None:
        print(f"Local execution message:\n[{typ}]: {data}")
        return

    response = requests.post(
        url=ADD_MESSAGE_ENDPOINT,
        json={
            "execution_token": execution_token,
            "task": {
                "project": task_project,
                "domain": task_domain,
                "name": task_name,
                "version": task_version,
            },
            "task_attempt_number": task_attempt_number,
            "task_array_index": array_index,
            "type": typ,
            "data": data,
        },
    )

    if response.status_code != 200:
        raise RuntimeError("Could not add task execution message to Latch.")
