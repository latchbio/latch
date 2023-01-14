"""
config.latch
~~~~~~~~~~~~
Platform wide configuration, eg. api endpoints, callback server ports...
"""

import os
from dataclasses import dataclass, fields, is_dataclass
from typing import Type, TypeVar

DOMAIN = os.environ.get("LATCH_SDK_DOMAIN", "latch.bio")
CONSOLE_URL = f"https://console.{DOMAIN}"
NUCLEUS_URL = f"https://nucleus.{DOMAIN}"

T = TypeVar("T")


@dataclass
class _DataAPI:
    begin_upload: str = "/sdk/initiate-multipart-upload"
    complete_upload: str = "/sdk/complete-multipart-upload"
    download: str = "/sdk/download"
    id: str = "/sdk/node-id"
    list: str = "/sdk/list"
    remove: str = "/sdk/rm"
    touch: str = "/sdk/touch"
    mkdir: str = "/sdk/mkdir"
    verify: str = "/sdk/verify"
    test_data: str = "/sdk/get-test-data-creds"


@dataclass
class _WorkflowAPI:
    upload_image: str = "/sdk/initiate-image-upload"
    get_image: str = "/sdk/get-image-from-task"
    register: str = "/sdk/register-workflow"
    list: str = "/sdk/get-wf"
    interface: str = "/sdk/wf-interface"
    graph: str = "/sdk/get-workflow-graph"
    check_version: str = "/sdk/check-workflow-version"
    get_latest: str = "/sdk/get-latest-version-new"
    preview: str = "/sdk/workflow-ui-preview"


@dataclass
class _ExecutionAPI:
    create: str = "/sdk/wf"
    list: str = "/sdk/get-executions"
    abort: str = "/sdk/abort-execution"
    logs: str = "/sdk/get-logs-for-node"
    exec: str = "/sdk/get-pod-exec-info"


@dataclass
class _UserAPI:
    jwt: str = "/sdk/access-jwt"
    list_workspaces: str = "/sdk/get-ws"
    get_secret: str = "/secrets/get"
    get_secret_local: str = "/secrets/get-local"


@dataclass
class _CentromereAPI:
    provision: str = "/sdk/provision-centromere"
    start_local_dev: str = "/sdk/initiate-local-development-session"
    stop_local_dev: str = "/sdk/close-local-development-session"


@dataclass
class _API:
    data: _DataAPI
    workflow: _WorkflowAPI
    execution: _ExecutionAPI
    user: _UserAPI
    centromere: _CentromereAPI


@dataclass
class _LatchConfig:
    api: _API
    dkr_repo: str = "812206152185.dkr.ecr.us-west-2.amazonaws.com"
    console_url: str = CONSOLE_URL
    nucleus_url: str = NUCLEUS_URL


def build_endpoints(x: Type[T] = _API) -> T:
    res = {}
    for field in fields(x):
        if is_dataclass(field.type):
            res[field.name] = build_endpoints(field.type)
        elif field.type is str:
            res[field.name] = NUCLEUS_URL + field.default
    return x(**res)


# singleton config instance
config = _LatchConfig(api=build_endpoints())
