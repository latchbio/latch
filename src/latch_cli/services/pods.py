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
from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
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
pod_templates_account_query = gql.gql(
    resources.files(__package__)
    .joinpath("gql/pod_templates_account.graphql")
    .read_text()
)
pod_templates_visible_query = gql.gql(
    resources.files(__package__)
    .joinpath("gql/pod_templates_visible.graphql")
    .read_text()
)
pod_template_by_id_query = gql.gql(
    resources.files(__package__).joinpath("gql/pod_template_by_id.graphql").read_text()
)
pod_template_source_pod_query = gql.gql(
    resources.files(__package__)
    .joinpath("gql/pod_template_source_pod.graphql")
    .read_text()
)
create_pod_template_mutation = gql.gql(
    resources.files(__package__).joinpath("gql/pod_template_create.graphql").read_text()
)
create_pod_template_version_mutation = gql.gql(
    resources.files(__package__)
    .joinpath("gql/pod_template_create_version.graphql")
    .read_text()
)
pod_template_access_add_mutation = gql.gql(
    resources.files(__package__)
    .joinpath("gql/pod_template_access_add.graphql")
    .read_text()
)
pod_template_access_remove_mutation = gql.gql(
    resources.files(__package__)
    .joinpath("gql/pod_template_access_remove.graphql")
    .read_text()
)
pod_template_publish_mutation = gql.gql(
    resources.files(__package__)
    .joinpath("gql/pod_template_publish.graphql")
    .read_text()
)

pod_ssh_poll_interval_seconds = 0.5
pod_ssh_max_wait_seconds = 600.0

pod_template_default_target_domain = "2"
pod_template_default_target_region = "us-west-2"
pod_template_region_choices = ("us-west-2", "us-east-1", "eu-central-1", "eu-west-1")

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
    cpu: int | None = Field(
        default=None, json_schema_extra={"template": 2, "units": "cores"}
    )
    memory: int | None = Field(
        default=None, json_schema_extra={"template": 8, "units": "GiB"}
    )
    gpu: int | None = Field(default=None, json_schema_extra={"template": 0})
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
    target_domain: str = Field(default="2", json_schema_extra={"template": "2"})
    template_version_id: str | None = Field(
        default=None, json_schema_extra={"template": None}
    )
    ldata_node_id: int | None = Field(
        default=None, json_schema_extra={"template": None}
    )

    @model_validator(mode="after")
    def _validate_resource_or_template(self):
        if self.template_version_id is not None:
            if self.cpu is not None or self.memory is not None or self.gpu is not None:
                raise ValueError(
                    "Must specify either resource limits or a template, not both"
                )

            return self

        if self.cpu is None or self.memory is None:
            raise ValueError("Must specify either resource limits, or a template")

        return self


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


class PodTemplateSource(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    provider_id: str | None = Field(default=None, alias="providerId")
    status: str | None = None
    infra_geo_partition: str | None = Field(default=None, alias="infraGeoPartition")
    pod_id: str | None = Field(default=None, alias="podId")
    source_pod_display_name: str | None = Field(
        default=None, alias="sourcePodDisplayName"
    )


class PodTemplateBackup(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    provider_id: str | None = Field(default=None, alias="providerId")
    infra_geo_partition: str | None = Field(default=None, alias="infraGeoPartition")


class PodTemplateBackupsConnection(BaseModel):
    nodes: list[PodTemplateBackup]


class PodTemplateBackupRequest(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    target_regions: list[str] = Field(default_factory=list, alias="targetRegions")
    pod_backups_by_req_id: PodTemplateBackupsConnection | None = Field(
        default=None, alias="podBackupsByReqId"
    )


class PodTemplateBackupRequestsConnection(BaseModel):
    nodes: list[PodTemplateBackupRequest]


class PodTemplateVersion(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    metadata: str
    creation_date: str | None = Field(default=None, alias="creationDate")
    status: str | None = None
    source: PodTemplateSource | None = None
    pod_backup_requests: PodTemplateBackupRequestsConnection | None = Field(
        default=None, alias="podBackupRequests"
    )


class PodTemplateVersionsConnection(BaseModel):
    nodes: list[PodTemplateVersion]


class PodTemplateSubscription(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    account_id: str = Field(alias="accountId")
    template_id: str = Field(alias="templateId")


class PodTemplateSubscriptionsConnection(BaseModel):
    nodes: list[PodTemplateSubscription]


class PodTemplate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    metadata: str
    available_in_explore: bool = Field(alias="availableInExplore")
    owner_id: str = Field(alias="ownerId")
    pod_template_versions_by_template_id: PodTemplateVersionsConnection = Field(
        alias="podTemplateVersionsByTemplateId"
    )
    pod_template_subscriptions_by_template_id: (
        PodTemplateSubscriptionsConnection | None
    ) = Field(default=None, alias="podTemplateSubscriptionsByTemplateId")


class PodTemplatesConnection(BaseModel):
    nodes: list[PodTemplate]


class PodTemplateSubscriptionWithTemplate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    account_id: str = Field(alias="accountId")
    template_id: str = Field(alias="templateId")
    template: PodTemplate


class PodTemplateSubscriptionsWithTemplateConnection(BaseModel):
    nodes: list[PodTemplateSubscriptionWithTemplate]


class PodTemplateAccountInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    pod_templates_by_owner_id: PodTemplatesConnection = Field(
        alias="podTemplatesByOwnerId"
    )
    pod_template_subscriptions_by_account_id: PodTemplateSubscriptionsWithTemplateConnection = Field(
        alias="podTemplateSubscriptionsByAccountId"
    )


class PodTemplatesAccountResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    account_info: PodTemplateAccountInfo | None = Field(alias="accountInfo")


class PodTemplatesVisibleResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    pod_templates: PodTemplatesConnection = Field(alias="podTemplates")


class PodTemplateByIdResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    pod_template: PodTemplate | None = Field(default=None, alias="podTemplate")


class CreatePodTemplateResponseTemplate(BaseModel):
    id: str


class CreatePodTemplatePayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    pod_template: CreatePodTemplateResponseTemplate | None = Field(
        default=None, alias="podTemplate"
    )


class CreatePodTemplateResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    create_pod_template: CreatePodTemplatePayload | None = Field(
        default=None, alias="createPodTemplate"
    )


class CreatePodTemplateVersionPayload(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str = Field(alias="bigInt")


class CreatePodTemplateVersionResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    pod_create_template_version: CreatePodTemplateVersionPayload | None = Field(
        default=None, alias="podCreateTemplateVersion"
    )


class SourcePodTemplateVersion(BaseModel):
    metadata: str | None = None


class SourcePodDeployment(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    target_region: str | None = Field(default=None, alias="targetRegion")


class SourcePodInfo(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    id: str
    display_name: str | None = Field(default=None, alias="displayName")
    metadata: Any = None
    cpu_millicores: int = Field(alias="cpuMillicores")
    memory_bytes: int = Field(alias="memoryBytes")
    storage_gigs: int = Field(alias="storageGigs")
    gpus: int = 0
    gpu_type: str | None = Field(default=None, alias="gpuType")
    template_version: SourcePodTemplateVersion | None = Field(
        default=None, alias="templateVersion"
    )
    deployment: SourcePodDeployment | None = None


class SourcePodResponse(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    pod_info: SourcePodInfo | None = Field(default=None, alias="podInfo")


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


def _json_object(value: object, *, context: str) -> dict[str, object]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception as e:
            raise click.ClickException(f"Invalid {context} metadata JSON.") from e

    if isinstance(value, dict):
        return value

    raise click.ClickException(f"Invalid {context} metadata.")


def _template_metadata(template: PodTemplate) -> dict[str, object]:
    return _json_object(template.metadata, context=f"template `{template.id}`")


def _template_display_name(template: PodTemplate) -> str:
    metadata = _template_metadata(template)
    name = metadata.get("displayName")
    if isinstance(name, str) and name != "":
        return name

    return f"Pod Template {template.id}"


def _version_metadata(version: PodTemplateVersion) -> dict[str, object]:
    return _json_object(version.metadata, context=f"template version `{version.id}`")


def _version_label(version: PodTemplateVersion) -> str | None:
    label = _version_metadata(version).get("version")
    return label if isinstance(label, str) else None


def _is_successful_template_version(version: PodTemplateVersion) -> bool:
    return version.status == "SUCCESS"


def _select_template_version(
    template: PodTemplate, *, version: str | None = None
) -> PodTemplateVersion:
    versions = template.pod_template_versions_by_template_id.nodes

    if version is None:
        for candidate in versions:
            if _is_successful_template_version(candidate):
                return candidate

        raise click.ClickException(
            f"Template `{_template_display_name(template)}` has no usable versions."
        )

    matches = [
        candidate for candidate in versions if _version_label(candidate) == version
    ]
    if len(matches) == 0:
        available_versions = [
            label
            for label in (_version_label(candidate) for candidate in versions)
            if label is not None
        ]
        suffix = (
            f" Available versions: {', '.join(available_versions)}."
            if len(available_versions) > 0
            else ""
        )
        raise click.ClickException(
            f"Template `{_template_display_name(template)}` has no version "
            f"`{version}`.{suffix}"
        )

    successful_matches = [
        candidate for candidate in matches if _is_successful_template_version(candidate)
    ]
    if len(successful_matches) == 0:
        raise click.ClickException(
            f"Template version `{version}` exists but is not ready."
        )

    if len(successful_matches) > 1:
        ids = ", ".join(candidate.id for candidate in successful_matches)
        raise click.ClickException(
            f"Template `{_template_display_name(template)}` has multiple usable "
            f"versions named `{version}`: {ids}."
        )

    return successful_matches[0]


def _default_region_for_template_version(version: PodTemplateVersion) -> str:
    if version.source is not None and version.source.infra_geo_partition is not None:
        return version.source.infra_geo_partition

    for request in (
        version.pod_backup_requests.nodes
        if version.pod_backup_requests is not None
        else []
    ):
        backups = request.pod_backups_by_req_id
        if backups is None:
            continue
        for backup in backups.nodes:
            if backup.infra_geo_partition is not None:
                return backup.infra_geo_partition

    return pod_template_default_target_region


def _template_json_summary(
    template: PodTemplate, *, workspace_id: str | None
) -> dict[str, object]:
    metadata = _template_metadata(template)
    versions: list[dict[str, object]] = []
    latest_successful_version: dict[str, object] | None = None
    latest_status = "-"

    for version in template.pod_template_versions_by_template_id.nodes:
        version_metadata = _version_metadata(version)
        label = version_metadata.get("version")
        row = {
            "id": version.id,
            "version": label,
            "status": version.status,
            "creation_date": version.creation_date,
        }
        versions.append(row)

        if latest_status == "-":
            latest_status = version.status or "-"

        if latest_successful_version is None and _is_successful_template_version(
            version
        ):
            latest_successful_version = row

    return {
        "id": template.id,
        "name": metadata.get("displayName"),
        "description": metadata.get("description"),
        "owner_id": template.owner_id,
        "published": template.available_in_explore,
        "shared": workspace_id is not None and template.owner_id != workspace_id,
        "latest_version": None
        if latest_successful_version is None
        else latest_successful_version.get("version"),
        "latest_version_id": None
        if latest_successful_version is None
        else latest_successful_version.get("id"),
        "latest_status": latest_status,
        "versions": versions,
    }


def _dedupe_templates(templates: list[PodTemplate]) -> list[PodTemplate]:
    res: list[PodTemplate] = []
    seen: set[str] = set()
    for template in templates:
        if template.id in seen:
            continue
        seen.add(template.id)
        res.append(template)

    return res


def _fetch_account_pod_templates(workspace_id: str) -> list[PodTemplate]:
    try:
        res = PodTemplatesAccountResponse.model_validate(
            gql_execute(pod_templates_account_query, {"accountId": workspace_id})
        )
    except ValidationError as e:
        raise click.ClickException(
            f"Malformed response while listing pod templates: {e}"
        ) from e

    if res.account_info is None:
        raise click.ClickException("Workspace does not exist or permission denied.")

    templates = list(res.account_info.pod_templates_by_owner_id.nodes)
    templates.extend(
        x.template
        for x in res.account_info.pod_template_subscriptions_by_account_id.nodes
    )
    return _dedupe_templates(templates)


def _fetch_visible_pod_templates() -> list[PodTemplate]:
    try:
        res = PodTemplatesVisibleResponse.model_validate(
            gql_execute(pod_templates_visible_query)
        )
    except ValidationError as e:
        raise click.ClickException(
            f"Malformed response while looking up pod templates: {e}"
        ) from e

    return res.pod_templates.nodes


def _fetch_pod_template_by_id(template_id: str) -> PodTemplate | None:
    try:
        res = PodTemplateByIdResponse.model_validate(
            gql_execute(pod_template_by_id_query, {"templateId": template_id})
        )
    except ValidationError as e:
        raise click.ClickException(
            f"Malformed response while looking up pod template: {e}"
        ) from e

    return res.pod_template


def _resolve_pod_template(template: str) -> PodTemplate:
    if template.isdecimal():
        res = _fetch_pod_template_by_id(template)
        if res is None:
            raise click.ClickException(
                f"Template `{template}` does not exist or permission denied."
            )
        return res

    matches = [
        candidate
        for candidate in _fetch_visible_pod_templates()
        if _template_display_name(candidate) == template
    ]
    if len(matches) == 0:
        raise click.ClickException(f"No visible pod template named `{template}`.")

    if len(matches) > 1:
        rows = ", ".join(f"{_template_display_name(x)} ({x.id})" for x in matches)
        raise click.ClickException(
            f"Multiple visible pod templates are named `{template}`: {rows}. "
            "Use the template id."
        )

    return matches[0]


def _post_create_pod(payload: dict[str, object]) -> None:
    from latch_cli.utils import get_auth_header

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

    _post_create_pod(payload)


def create_pod_from_template(
    *,
    template: str,
    version: str | None = None,
    display_name: str | None = None,
    workspace_id: str | None = None,
    backup_interval: Literal["daily", "weekly", "monthly"] | None = None,
    target_region: Literal["us-west-2", "us-east-1", "eu-central-1", "eu-west-1"]
    | None = None,
    target_domain: str | None = None,
) -> None:
    if workspace_id is None:
        try:
            workspace_id = current_workspace()
        except NoWorkspaceSelectedError as e:
            raise click.ClickException(str(e)) from e

    pod_template = _resolve_pod_template(template)
    pod_template_version = _select_template_version(pod_template, version=version)
    template_name = _template_display_name(pod_template)

    payload: dict[str, object] = {
        "ws_account_id": workspace_id,
        "display_name": display_name if display_name is not None else template_name,
        "template_version_id": pod_template_version.id,
        "target_region": target_region
        if target_region is not None
        else _default_region_for_template_version(pod_template_version),
        "target_domain": target_domain or pod_template_default_target_domain,
    }
    if backup_interval is not None:
        payload["backup_interval"] = backup_interval

    _post_create_pod(payload)


def list_pod_templates(*, json_output: bool = False) -> None:
    workspace_id = current_workspace()
    templates = _fetch_account_pod_templates(workspace_id)
    summaries = [
        _template_json_summary(template, workspace_id=workspace_id)
        for template in templates
    ]

    if json_output:
        click.echo(json.dumps(summaries, indent=2, sort_keys=True))
        return

    if len(summaries) == 0:
        click.echo("No pod templates found.")
        return

    table = Table(show_header=True, header_style="bold underline", box=None)
    for field_name in [
        "id",
        "name",
        "latest_version",
        "latest_status",
        "published",
        "owner_id",
        "shared",
    ]:
        table.add_column(field_name)

    for row in summaries:
        table.add_row(
            str(row["id"]),
            str(row.get("name") or "-"),
            str(row.get("latest_version") or "-"),
            str(row.get("latest_status") or "-"),
            "yes" if row.get("published") is True else "no",
            str(row.get("owner_id") or "-"),
            "yes" if row.get("shared") is True else "no",
        )

    Console().print(table)


def _trigger_pod_backup(pod_id: int) -> str:
    from latch_cli.utils import get_auth_header

    res = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/pods/trigger-backup"),
        headers={"Authorization": get_auth_header()},
        json={"pod_id": str(pod_id)},
    )

    try:
        body = res.json()
    except Exception as e:
        raise click.ClickException(
            "Malformed response while creating pod backup."
        ) from e

    if not isinstance(body, dict):
        raise click.ClickException("Malformed response while creating pod backup.")

    provider_id = body.get("providerId")
    if res.status_code == 200 and isinstance(provider_id, str) and provider_id != "":
        return provider_id

    err = body.get("error")
    if isinstance(err, str) and err != "":
        raise click.ClickException(f"Unable to create pod backup: {err}")

    if res.status_code in {403, 404}:
        raise click.ClickException("Pod does not exist or permission denied.")

    raise click.ClickException(
        "Internal error while creating pod backup. Please try again. "
        "contact `support@latch.bio` if the issue persists."
    )


def _fetch_source_pod(pod_id: int) -> SourcePodInfo:
    try:
        res = SourcePodResponse.model_validate(
            gql_execute(pod_template_source_pod_query, {"podId": str(pod_id)})
        )
    except ValidationError as e:
        raise click.ClickException(
            f"Malformed response while looking up pod `{pod_id}`: {e}"
        ) from e

    if res.pod_info is None:
        raise click.ClickException(
            f"Pod `{pod_id}` does not exist or permission denied."
        )

    return res.pod_info


def _pod_metadata_for_template(pod: SourcePodInfo) -> dict[str, object]:
    if pod.metadata is None:
        return {}

    if isinstance(pod.metadata, dict):
        return pod.metadata

    return _json_object(pod.metadata, context=f"pod `{pod.id}`")


def _pod_is_windows(pod: SourcePodInfo) -> bool:
    version_metadata_raw = None
    if pod.template_version is not None:
        version_metadata_raw = pod.template_version.metadata
    if version_metadata_raw is None:
        return False

    version_metadata = _json_object(
        version_metadata_raw, context="source template version"
    )
    return version_metadata.get("windows") is True


def _template_target_regions(
    pod: SourcePodInfo, requested_regions: tuple[str, ...]
) -> list[str]:
    source_region = pod_template_default_target_region
    if pod.deployment is not None and pod.deployment.target_region is not None:
        source_region = pod.deployment.target_region

    res: list[str] = []
    for region in (source_region, *requested_regions):
        if region not in res:
            res.append(region)

    return res


def save_pod_template(
    *,
    pod_id: int,
    template: str | None = None,
    name: str | None = None,
    description: str | None = None,
    version: str,
    notes: str | None = None,
    target_regions: tuple[str, ...] = (),
) -> None:
    if version.strip() == "":
        raise click.UsageError("Provide a non-empty --version.")

    if template is None:
        if name is None or name.strip() == "":
            raise click.UsageError("Provide --name when creating a new template.")
        if description is None or description.strip() == "":
            raise click.UsageError(
                "Provide --description when creating a new template."
            )
    elif name is not None or description is not None:
        raise click.UsageError(
            "Use --template to add a version to an existing template, or "
            "--name/--description to create a new template."
        )

    existing_template_id = None
    if template is not None:
        existing_template_id = _resolve_pod_template(template).id

    pod = _fetch_source_pod(pod_id)
    backup_provider_id = _trigger_pod_backup(pod_id)

    version_metadata: dict[str, object] = {
        "version": version.strip(),
        "podMetadata": _pod_metadata_for_template(pod),
        "cpuMillicores": pod.cpu_millicores,
        "memoryBytes": pod.memory_bytes,
        "storageGigs": pod.storage_gigs,
        "gpus": pod.gpus,
        "windows": _pod_is_windows(pod),
    }
    if pod.gpu_type is not None:
        version_metadata["gpuType"] = pod.gpu_type
    if notes is not None and notes.strip() != "":
        version_metadata["notes"] = notes.strip()

    regions = _template_target_regions(pod, target_regions)
    version_metadata_json = json.dumps(version_metadata)

    if template is None:
        workspace_id = current_workspace()
        template_metadata = {
            "displayName": name.strip() if name is not None else "",
            "description": description.strip() if description is not None else "",
        }

        try:
            create_template_res = CreatePodTemplateResponse.model_validate(
                gql_execute(
                    create_pod_template_mutation,
                    {
                        "ownerId": workspace_id,
                        "metadata": json.dumps(template_metadata),
                    },
                )
            )
        except ValidationError as e:
            raise click.ClickException(
                f"Malformed response while creating pod template: {e}"
            ) from e

        created_template = create_template_res.create_pod_template
        if created_template is None or created_template.pod_template is None:
            raise click.ClickException(
                "Malformed response while creating pod template."
            )

        template_id = created_template.pod_template.id
    else:
        assert existing_template_id is not None
        template_id = existing_template_id

    try:
        create_version_res = CreatePodTemplateVersionResponse.model_validate(
            gql_execute(
                create_pod_template_version_mutation,
                {
                    "templateId": template_id,
                    "metadata": version_metadata_json,
                    "sourceId": backup_provider_id,
                    "targetRegions": regions,
                },
            )
        )
    except ValidationError as e:
        raise click.ClickException(
            f"Malformed response while creating pod template version: {e}"
        ) from e

    created_version = create_version_res.pod_create_template_version
    if created_version is None:
        raise click.ClickException(
            "Malformed response while creating pod template version."
        )

    click.echo(
        json.dumps(
            {
                "template_id": template_id,
                "template_version_id": created_version.id,
                "backup_provider_id": backup_provider_id,
            },
            sort_keys=True,
        )
    )


def update_pod_template_access(
    *,
    template_id: str,
    add_workspace_ids: tuple[str, ...] = (),
    remove_workspace_ids: tuple[str, ...] = (),
) -> None:
    if len(add_workspace_ids) == 0 and len(remove_workspace_ids) == 0:
        raise click.UsageError("Provide at least one --add or --remove workspace id.")

    full_template = _fetch_pod_template_by_id(template_id)
    if full_template is None:
        raise click.ClickException(
            f"Template `{template_id}` does not exist or permission denied."
        )

    subscriptions = full_template.pod_template_subscriptions_by_template_id
    existing = (
        {subscription.account_id for subscription in subscriptions.nodes}
        if subscriptions is not None
        else set()
    )

    added: list[str] = []
    skipped_add: list[str] = []
    for workspace_id in add_workspace_ids:
        if workspace_id in existing:
            skipped_add.append(workspace_id)
            continue

        gql_execute(
            pod_template_access_add_mutation,
            {"templateId": full_template.id, "accountId": workspace_id},
        )
        existing.add(workspace_id)
        added.append(workspace_id)

    removed: list[str] = []
    skipped_remove: list[str] = []
    for workspace_id in remove_workspace_ids:
        if workspace_id not in existing:
            skipped_remove.append(workspace_id)
            continue

        gql_execute(
            pod_template_access_remove_mutation,
            {"templateId": full_template.id, "accountId": workspace_id},
        )
        existing.remove(workspace_id)
        removed.append(workspace_id)

    click.echo(
        json.dumps(
            {
                "template_id": full_template.id,
                "added": added,
                "removed": removed,
                "skipped_add": skipped_add,
                "skipped_remove": skipped_remove,
            },
            sort_keys=True,
        )
    )


def set_pod_template_publish_status(*, template: str, publish: bool) -> None:
    pod_template = _resolve_pod_template(template)
    action = "publish" if publish else "unpublish"

    try:
        gql_execute(
            pod_template_publish_mutation,
            {"templateId": pod_template.id, "availableInExplore": publish},
        )
    except Exception as e:
        msg = str(e)
        if "violates row-level security" in msg:
            raise click.ClickException(
                f"Unable to {action} pod template: public pod template publishing "
                "may require Latch approval."
            ) from e
        raise

    click.echo(
        json.dumps(
            {"template_id": pod_template.id, "published": publish}, sort_keys=True
        )
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
    target_region = (
        deployment.get("targetRegion", "us-west-2")
        if deployment.get("targetRegion") is not None
        else "us-west-2"
    )
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
