from __future__ import annotations

import json
import os
import shlex
import time
from importlib import resources
from typing import TYPE_CHECKING, Any
from urllib.parse import urljoin

import click
import gql
from pydantic import BaseModel, ConfigDict, Field, ValidationError
from rich.console import Console
from rich.table import Table
from typing_extensions import Literal

from latch.utils import NoWorkspaceSelectedError, current_workspace
from latch_cli import tinyrequests
from latch_sdk_config.latch import NUCLEUS_URL
from latch_sdk_gql.execute import execute as gql_execute

if TYPE_CHECKING:
    from collections.abc import Callable
    from pathlib import Path

pods_query = gql.gql(
    resources.files(__package__).joinpath("gql/pods_list.graphql").read_text()
)
pod_ssh_query = gql.gql(
    resources.files(__package__).joinpath("gql/pod_ssh.graphql").read_text()
)

pod_ssh_poll_interval_seconds = 0.5
pod_ssh_max_wait_seconds = 600.0

pod_ssh_starting_statuses = {
    "STARTING",
    "POD_SCHEDULED",
    "INITIALIZED",
    "POD_HAS_NETWORK",
    "CONTAINERS_READY",
}


class CreatePodRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ws_account_id: str | None = Field(
        default=None, json_schema_extra={"default_description": "current workspace"}
    )
    display_name: str = Field(..., json_schema_extra={"template": "My Pod"})
    cpu: int = Field(..., json_schema_extra={"template": 2, "units": "cores"})
    memory: int = Field(..., json_schema_extra={"template": 8, "units": "GiB"})
    gpu: int = Field(default=0, json_schema_extra={"template": 0})
    gpu_type: Literal["nvidia-a10g", "nvidia-l40s"] | None = Field(
        default=None, json_schema_extra={"template": None}
    )
    storage_gigs: int = Field(
        default=20, json_schema_extra={"template": 20, "units": "GiB"}
    )
    backup_interval: Literal["daily", "weekly", "monthly"] | None = Field(
        default=None, json_schema_extra={"template": None}
    )
    target_region: Literal["us-west-2", "us-east-1", "eu-central-1", "eu-west-1"] = (
        Field(default="us-west-2", json_schema_extra={"template": "us-west-2"})
    )


class PodInfoList(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: object = Field(
        default=None, alias="id", json_schema_extra={"default_column": True}
    )
    creation_date: object = Field(default=None, alias="creationDate")
    display_name: object = Field(
        default=None, alias="displayName", json_schema_extra={"default_column": True}
    )
    status: object = Field(
        default=None, alias="status", json_schema_extra={"default_column": True}
    )
    cpu_millicores: object = Field(default=None, alias="cpuMillicores")
    memory_bytes: object = Field(default=None, alias="memoryBytes")
    gpus: object = Field(default=None, alias="gpus")
    gpu_type: object = Field(default=None, alias="gpuType")
    storage_gigs: object = Field(default=None, alias="storageGigs")
    used_storage_gigs: object = Field(default=None, alias="usedStorageGigs")
    archival_status: object = Field(default=None, alias="archivalStatus")
    backup_interval: object = Field(default=None, alias="backupInterval")
    auto_shutoff_delay: object = Field(default=None, alias="autoShutoffDelay")
    effective_security_metadata: object = Field(
        default=None, alias="effectiveSecurityMetadata"
    )
    latest_pod_session: object = Field(default=None, alias="latestPodSession")
    deployment: object = Field(default=None, alias="deployment")


class PodInfosConnection(BaseModel):
    nodes: list[PodInfoList]


class PodInfosResponse(BaseModel):
    pod_infos: PodInfosConnection = Field(alias="podInfos")


class PodSshResponse(BaseModel):
    pod_info: dict[str, Any] | None = Field(alias="podInfo")


def pod_list_fields(*, detailed: bool) -> list[str]:
    res: list[str] = []
    for field_info in PodInfoList.model_fields.values():
        extra = field_info.json_schema_extra
        if not detailed and (
            not isinstance(extra, dict) or extra.get("default_column") is not True
        ):
            continue

        alias = field_info.alias
        if isinstance(alias, str):
            res.append(alias)

    return res


def create_pod_request_skeleton() -> dict[str, object]:
    res: dict[str, object] = {}
    for field_name, field_info in CreatePodRequest.model_fields.items():
        extra = field_info.json_schema_extra
        if not isinstance(extra, dict) or "template" not in extra:
            continue

        res[field_name] = extra["template"]

    return res


forch_pods_ssh_endpoint_by_domain_region = {
    "1": {"us-west-2": "54.212.151.84"},
    "2": {
        "us-west-2": "44.237.115.144",
        "us-east-1": "52.0.156.72",
        "eu-central-1": "3.72.154.205",
        "eu-west-1": "54.154.243.51",
    },
}


def create_pod(request_file: Path) -> None:
    from latch_cli.utils import get_auth_header

    try:
        payload_model = CreatePodRequest.model_validate_json(
            request_file.read_text(encoding="utf-8")
        )
    except ValidationError as e:
        raise click.ClickException(f"Invalid pod create request: {e}") from e

    if payload_model.ws_account_id is None:
        try:
            payload_model.ws_account_id = current_workspace()
        except NoWorkspaceSelectedError as e:
            raise click.ClickException(str(e)) from e

    payload = payload_model.model_dump(exclude_none=True)

    res = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/pods/create"),
        headers={"Authorization": get_auth_header()},
        json=payload,
    )

    try:
        body = res.json()
    except Exception as e:
        raise click.ClickException("Malformed response while creating pod.") from e

    if not isinstance(body, dict):
        raise click.ClickException("Malformed response while creating pod.")

    pod_id = body.get("id")
    if res.status_code == 200 and pod_id is not None:
        click.echo(json.dumps({"pod_id": pod_id}))
        return

    err = body.get("error")
    if isinstance(err, str) and err != "":
        raise click.ClickException(f"Unable to create pod: {err}")

    if res.status_code in {403, 404}:
        raise click.ClickException("Permission denied.")
    raise click.ClickException(
        "Internal error while creating pod. Please try again. "
        "contact `support@latch.bio` if the issue persists."
    )


def list_pods(*, detailed: bool = False) -> None:
    from latch.utils import current_workspace

    workspace_id = current_workspace()
    try:
        res = PodInfosResponse.model_validate(
            gql_execute(pods_query, {"accountId": workspace_id})
        )
    except ValidationError as e:
        raise click.ClickException(f"Malformed response while listing pods: {e}") from e

    pods = res.pod_infos.nodes

    output_fields = pod_list_fields(detailed=detailed)
    output: list[dict[str, object]] = []
    for pod in pods:
        row = pod.model_dump(by_alias=True)
        output.append({field_name: row.get(field_name) for field_name in output_fields})

    if detailed:
        click.echo(json.dumps(output, indent=2, sort_keys=True))
        return

    if len(output) == 0:
        click.echo("No pods found.")
        return

    table = Table(show_header=True, header_style="bold underline", box=None)
    for field_name in output_fields:
        table.add_column(field_name)

    for row in output:
        rendered_row = []
        for field_name in output_fields:
            value = row.get(field_name)
            if value is None:
                rendered_row.append("-")
            elif isinstance(value, (dict, list)):
                rendered_row.append(
                    json.dumps(value, separators=(",", ":"), sort_keys=True)
                )
            else:
                rendered_row.append(str(value))

        table.add_row(*rendered_row)

    Console().print(table)


def start_pod(pod_id: int) -> None:
    from latch_cli.utils import get_auth_header

    res = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/pods/start"),
        headers={"Authorization": get_auth_header()},
        json={"pod_id": str(pod_id)},
    )

    try:
        body = res.json()
    except Exception as e:
        raise click.ClickException("Malformed response while starting pod.") from e

    if not isinstance(body, dict):
        raise click.ClickException("Malformed response while starting pod.")

    if res.status_code == 200 and body.get("success") is True:
        click.secho(f"Pod with ID `{pod_id}` started.", fg="green")
        return

    err = body.get("error")
    source = err.get("source") if isinstance(err, dict) else err

    if source == "out_of_credits":
        click.secho("Unable to start pod: workspace is out of credits.", fg="red")
        return

    if source == "usage_limit_reached":
        click.secho("Unable to start pod: usage limit reached.", fg="red")
        return

    if isinstance(err, str) and err != "":
        click.secho(f"Unable to start pod: {err}", fg="red")
        return

    if res.status_code in {403, 404}:
        click.secho("Pod does not exist or permission denied.", fg="red")
        return

    click.secho(
        f"Internal error while starting Pod `{pod_id}`. Please try again. ", fg="red"
    )


def _format_pod_status(status: str) -> str:
    return status.lower().replace("_", " ")


def _get_pod_ssh_args(
    pod_id: int, pod: dict[str, object], *, key: Path | None = None
) -> list[str] | None:
    deployment = pod.get("deployment")
    if not isinstance(deployment, dict):
        click.secho("Pod deployment information is unavailable.", fg="red")
        return None

    target_domain = deployment.get("targetDomain")
    target_region = deployment.get("targetRegion", "us-west-2") if deployment.get("targetRegion") is not None else "us-west-2"
    if not isinstance(target_domain, str) or not isinstance(target_region, str):
        click.secho("Pod deployment information is unavailable.", fg="red")
        return None

    jump_host = forch_pods_ssh_endpoint_by_domain_region.get(target_domain, {}).get(
        target_region
    )
    if jump_host is None:
        click.secho(
            "Pod SSH is unavailable for deployment "
            f"domain `{target_domain}` region `{target_region}`.",
            fg="red",
        )
        return None

    ssh_args = ["ssh", "-o", "ServerAliveInterval=30", "-o", "ServerAliveCountMax=5"]
    if key is not None:
        ssh_args.extend(["-i", str(key)])

    ssh_args.extend(["-J", f"root@{jump_host}", f"root@{pod_id}.pods-ssh.latch.bio"])
    return ssh_args


def ssh_pod(
    pod_id: int,
    *,
    print_only: bool = False,
    key: Path | None = None,
    poll_interval_seconds: float = pod_ssh_poll_interval_seconds,
    max_wait_seconds: float = pod_ssh_max_wait_seconds,
    exec_fn: Callable[[str, list[str]], object] = os.execvp,
) -> None:
    deadline = time.monotonic() + max_wait_seconds
    waiting_message_printed = False

    while True:
        try:
            res = PodSshResponse.model_validate(
                gql_execute(pod_ssh_query, {"podId": str(pod_id)})
            )
        except ValidationError as e:
            raise click.ClickException(
                f"Malformed response while checking pod SSH status: {e}"
            ) from e
        pod = res.pod_info
        if pod is None:
            click.secho("Pod does not exist or permission denied.", fg="red")
            return
        if not isinstance(pod, dict):
            click.secho("Pod does not exist or permission denied.", fg="red")
            return

        status_value = pod.get("status")
        status = status_value.upper() if isinstance(status_value, str) else "UNKNOWN"

        if print_only:
            ssh_args = _get_pod_ssh_args(pod_id, pod, key=key)
            if ssh_args is None:
                return

            click.echo(shlex.join(ssh_args))
            return

        if status == "STOPPED":
            click.secho(
                f"Pod `{pod_id}` is stopped. Start it first with "
                f"`latch pods start {pod_id}`.",
                fg="yellow",
            )
            return

        if status == "RUNNING":
            ssh_args = _get_pod_ssh_args(pod_id, pod, key=key)
            if ssh_args is None:
                return

            click.echo(shlex.join(ssh_args))
            # Allow time for ssh forwarder ip list to propagate
            time.sleep(3)
            exec_fn(ssh_args[0], ssh_args)
            return

        if status in pod_ssh_starting_statuses:
            if time.monotonic() >= deadline:
                click.secho(f"Timed out waiting for pod `{pod_id}` to start.", fg="red")
                return

            if not waiting_message_printed:
                click.echo(
                    f"Pod `{pod_id}` is {_format_pod_status(status)}. "
                    "Waiting for it to start..."
                )
                waiting_message_printed = True

            time.sleep(
                min(poll_interval_seconds, max(0.0, deadline - time.monotonic()))
            )
            continue

        click.secho(
            f"Pod `{pod_id}` is {_format_pod_status(status)}. "
            "SSH is only available once the pod is running.",
            fg="red",
        )
        return


def stop_pod(pod_id: int) -> None:
    from latch_cli.utils import get_auth_header

    res = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/pods/stop"),
        headers={"Authorization": get_auth_header()},
        json={"pod_id": pod_id},
    )

    if res.status_code == 200:
        click.secho(f"Pod with ID `{pod_id}` stopped.", fg="green")
        return

    if res.status_code in {403, 404}:
        click.secho("Pod does not exist or permission denied.", fg="red")
        return

    click.secho(
        f"Internal error while stopping Pod `{pod_id}`. Please try again. "
        "contact `support@latch.bio` if the issue persists.",
        fg="red",
    )
