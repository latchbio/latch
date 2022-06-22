import os as _os
from typing import Any, Dict

import requests


LOCAL_EXECUTION_MODULE_NAME = "__main__"
FLYTE_EXECUTION_MODULE_NAME = "wf.__init__"
ADD_TASK_EXECUTION_MESSAGE = "https://nucleus.latch.bio/api/add-task-execution-message"


def message(level: str, data: Dict[str, Any]) -> None:
    if __name__ == LOCAL_EXECUTION_MODULE_NAME:
        print(
            "Printing message in local execution mode; to test on the cloud, "
            "register, then run this workflow on the LatchBio Console."
        )
        print(f"[{level}]: {data}")
        return

    if __name__ != FLYTE_EXECUTION_MODULE_NAME:
        raise RuntimeError("Invalid runtime")

    try:
        task_name = _os.environ["FLYTE_INTERNAL_TASK_NAME"]
        task_version = _os.environ["FLYTE_INTERNAL_TASK_VERSION"]
        execution_token = _os.environ["FLYTE_INTERNAL_EXECUTION_ID"]
    except KeyError as e:
        raise RuntimeError("Unset Flyte environment variables") from e

    response = requests.post(
        url=ADD_TASK_EXECUTION_MESSAGE,
        json={
            "execution_token": execution_token,
            "task_name": task_name,
            "task_version": task_version,
            "level": level,
            "data": data,
        },
    )

    if response.status_code != 200:
        print("Error adding task execution message to Latch, printing below.")

    print(f"[{level}]: {data}")
