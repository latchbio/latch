import json
import logging
import time
from pathlib import Path
from typing import List, OrderedDict

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from latch_cli.utils import account_id_from_token, retrieve_or_login

logger = logging.Logger(name="logger")

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


class MetadataEventHandler(FileSystemEventHandler):
    def __init__(self, workflow_name, headers, account_id):
        self.workflow_name = workflow_name
        self.headers = headers
        self.account_id = account_id
        self.logger = logging.Logger(name="Watch Logger", level=logging.WARNING)

    def on_modified(self, event):

        import requests
        from flytekit.clis.sdk_in_container.run import load_naive_entity
        from flytekit.models.core import identifier as _identifier_model
        from flytekit.models.core import workflow as _workflow_model

        try:
            wf = load_naive_entity("wf.__init__", self.workflow_name)
        except:
            raise ValueError(
                f"Unable to find wf.__init__.{self.workflow_name}"
                " - make sure that the function names match."
            )

        # closure = workflow_pb2.WorkflowClosure()
        # closure.ParseFromString(str(workflow.interface.to_flyte_idl()).encode("utf-8"))
        # wf = closure.compiled_workflow
        # iface_idl = wf.primary.template.interface

        # self.logger.warning(iface_idl)

        project = ""
        domain = ""
        version = ""

        wf_id = _identifier_model.Identifier(
            resource_type=_identifier_model.ResourceType.WORKFLOW,
            project=project,
            domain=domain,
            name=wf.name,
            version=version,
        )
        wf_t = _workflow_model.WorkflowTemplate(
            id=wf_id,
            metadata=wf.workflow_metadata.to_flyte_model(),
            metadata_defaults=wf.workflow_metadata_defaults.to_flyte_model(),
            interface=wf.interface,
            nodes=[],
            outputs=wf.output_bindings,
        )

        def deep_dict(t) -> dict:
            if hasattr(t, "__dict__"):
                output = {}
                for k in t.__dict__:
                    if t.__dict__[k] is not None:
                        new_key = k.strip("_")
                        if new_key == "simple":
                            val = SIMPLE_MAP.get(t.__dict__[k], None)
                        elif new_key == "dimensionality":
                            val = DIM_MAP.get(t.__dict__[k], None)
                        else:
                            val = t.__dict__[k]
                        output[new_key] = deep_dict(val)
                return output
            elif isinstance(t, List):
                output = []
                for i in range(len(t)):
                    if t[i] is not None:
                        output.append(deep_dict(t[i]))
                return output
            else:
                return t

        d = {k: deep_dict(wf_t.interface.inputs[k]) for k in wf_t.interface.inputs}

        param_str = json.dumps(
            {"variables": d},
            sort_keys=True,
            indent=2,
        )

        self.logger.warning(param_str)

        resp = requests.post(
            url="https://nucleus.latch.bio/sdk/workflow-ui-preview",
            headers=self.headers,
            json={"workflow_ui_preview": param_str},
        )

        resp.raise_for_status()


def watch(workflow_name: str):

    token = retrieve_or_login()
    account_id = account_id_from_token(token)
    headers = {"Authorization": f"Bearer {token}"}

    observer = Observer()
    handler = MetadataEventHandler(
        workflow_name=workflow_name, headers=headers, account_id=account_id
    )
    observer.schedule(handler, Path.cwd(), recursive=True)
    observer.start()
    try:
        while True:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
