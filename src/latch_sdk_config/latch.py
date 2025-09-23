"""config.latch
~~~~~~~~~~~~
Platform wide configuration, eg. api endpoints, callback server ports...
"""

# todo(ayush): put all configs into a `latch-config` package

import os
from dataclasses import dataclass, fields, is_dataclass
from typing import Type, TypeVar
from urllib.parse import urljoin

DOMAIN = os.environ.get("LATCH_SDK_DOMAIN", "latch.bio")
CONSOLE_URL: str = f"https://console.{DOMAIN}"
NUCLEUS_URL: str = f"https://nucleus.{DOMAIN}"
VACUOLE_URL: str = f"https://vacuole.{DOMAIN}"

T = TypeVar("T")


@dataclass
class _DataAPI:
    id: str = "/sdk/node-id"
    list: str = "/sdk/list"
    remove: str = "/sdk/rm"
    touch: str = "/sdk/touch"
    mkdir: str = "/sdk/mkdir"
    verify: str = "/sdk/verify"
    test_data: str = "/sdk/get-test-data-creds"

    get_signed_url: str = "/ldata/get-signed-url"
    get_signed_urls_recursive: str = "/ldata/get-signed-urls-recursive"
    start_upload: str = "/ldata/start-upload"
    end_upload: str = "/ldata/end-upload"


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
class _ConsoleRoutes:
    developer: str = urljoin(CONSOLE_URL, "/settings/developer")


@dataclass(frozen=True)
class _LatchConfig:
    api: _API
    console_routes: _ConsoleRoutes
    gql: str = f"{VACUOLE_URL}/graphql"
    dkr_repo: str = "812206152185.dkr.ecr.us-west-2.amazonaws.com"
    console_url: str = CONSOLE_URL
    nucleus_url: str = NUCLEUS_URL
    vacuole_url: str = VACUOLE_URL


def build_endpoints(x: Type[T]) -> T:
    res = {}
    for field in fields(x):
        if is_dataclass(field.type):
            res[field.name] = build_endpoints(field.type)
        elif field.type is str:
            res[field.name] = urljoin(NUCLEUS_URL, str(field.default))
    return x(**res)


# singleton config instance
config = _LatchConfig(api=build_endpoints(_API), console_routes=_ConsoleRoutes())
