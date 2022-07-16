import json
import logging
import webbrowser
from typing import List

from flytekit.clis.sdk_in_container.run import load_naive_entity

from latch_cli.config.latch import LatchConfig
from latch_cli.tinyrequests import post
from latch_cli.utils import retrieve_or_login

logger = logging.Logger(name="logger")
config = LatchConfig()
endpoints = config.sdk_endpoints

SIMPLE_MAP = {
    0: "NONE",
    1: "INTEGER",
    2: "FLOAT",
    3: "STRING",
    4: "BOOLEAN",
    5: "DATETIME",
    6: "DURATION",
    7: "BINARY",
    8: "ERROR",
    9: "STRUCT",
}

DIM_MAP = {
    0: "SINGLE",
    1: "MULTIPART",
}


def _deep_dict(t) -> dict:
    if hasattr(t, "__dict__"):
        output = {}
        for k in t.__dict__:
            if t.__dict__[k] is not None:
                new_key = k.strip("_")
                if new_key == "union_type":
                    new_key = "unionType"
                elif new_key == "collection_type":
                    new_key = "collectionType"
                elif new_key == "enum_type":
                    new_key = "enumType"

                if new_key == "simple":
                    val = SIMPLE_MAP.get(t.__dict__[k], None)
                elif new_key == "dimensionality":
                    val = DIM_MAP.get(t.__dict__[k], None)
                else:
                    val = t.__dict__[k]
                output[new_key] = _deep_dict(val)
        return output
    elif isinstance(t, List):
        output = []
        for i in range(len(t)):
            if t[i] is not None:
                output.append(_deep_dict(t[i]))
        return output
    else:
        return t


def preview(workflow_name: str):

    try:
        wf = load_naive_entity("wf.__init__", workflow_name)
    except:
        raise ValueError(
            f"Unable to find wf.__init__.{workflow_name}"
            " - make sure that the function names match."
        )

    d = {k: _deep_dict(wf.interface.inputs[k]) for k in wf.interface.inputs}

    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}

    resp = post(
        url=endpoints["preview"],
        headers=headers,
        json={
            "workflow_ui_preview": json.dumps(
                {"variables": d},
                sort_keys=True,
                indent=2,
            )
        },
    )

    resp.raise_for_status()

    url = f"{config.console_url}/preview/parameters"
    webbrowser.open(url)
