import os as _os
from typing import Any, Dict

import requests


NUCLEUS_URL = _os.environ.get("LATCH_CLI_NUCLEUS_URL", "https://nucleus.latch.bio")
ADD_MESSAGE_ENDPOINT = f"{NUCLEUS_URL}/add-task-execution-message"


def message(typ: str, data: Dict[str, Any]) -> None:
    try:
        task_name = _os.environ["FLYTE_INTERNAL_TASK_NAME"]
        task_version = _os.environ["FLYTE_INTERNAL_TASK_VERSION"]
        execution_token = _os.environ["FLYTE_INTERNAL_EXECUTION_ID"]
    except KeyError:
        print(f"Local execution message:\n[{typ}]: {data}")
        return

    response = requests.post(
        url=ADD_MESSAGE_ENDPOINT,
        json={
            "execution_token": execution_token,
            "task_name": task_name,
            "task_version": task_version,
            "type": typ,
            "data": data,
        },
    )

    if response.status_code != 200:
        raise RuntimeError("Could not add task execution message to Latch.")
