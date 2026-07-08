# ruff: noqa: PLC2701

import json
from typing import Any

import click
import pytest
from pydantic import ValidationError

from latch_cli.services.pods import (
    CreatePodRequest,
    PodTemplate,
    SourcePodInfo,
    _default_region_for_template_version,
    _select_template_version,
    _template_target_regions,
)


def _template(version_rows: list[dict[str, Any]]) -> PodTemplate:
    return PodTemplate.model_validate({
        "id": "10",
        "metadata": json.dumps({"displayName": "Example Template"}),
        "availableInExplore": False,
        "ownerId": "20",
        "podTemplateVersionsByTemplateId": {"nodes": version_rows},
    })


def _version(
    version_id: str, version: str, status: str = "SUCCESS", region: str = "us-east-1"
) -> dict[str, Any]:
    return {
        "id": version_id,
        "metadata": json.dumps({"version": version}),
        "creationDate": "2026-01-01T00:00:00+00:00",
        "status": status,
        "source": {
            "providerId": f"snap-{version_id}",
            "status": "SUCCESS",
            "infraGeoPartition": region,
            "podId": "1",
            "sourcePodDisplayName": "Source",
        },
        "podBackupRequests": {"nodes": []},
    }


def test_create_pod_request_accepts_template_without_resources():
    req = CreatePodRequest.model_validate({
        "display_name": "From template",
        "template_version_id": "123",
    })

    payload = req.model_dump(exclude_none=True)
    assert payload["template_version_id"] == "123"
    assert "cpu" not in payload
    assert "memory" not in payload
    assert "gpu" not in payload


def test_create_pod_request_rejects_template_and_resources():
    with pytest.raises(ValidationError):
        CreatePodRequest.model_validate({
            "display_name": "From template",
            "template_version_id": "123",
            "cpu": 2,
            "memory": 8,
        })


def test_select_template_version_defaults_to_latest_successful():
    template = _template([
        _version("2", "v2.0.0", status="PENDING"),
        _version("1", "v1.0.0", status="SUCCESS"),
    ])

    assert _select_template_version(template).id == "1"


def test_select_template_version_uses_version_label():
    template = _template([_version("2", "v2.0.0"), _version("1", "v1.0.0")])

    assert _select_template_version(template, version="v1.0.0").id == "1"


def test_select_template_version_rejects_unknown_label():
    template = _template([_version("1", "v1.0.0")])

    with pytest.raises(click.ClickException):
        _select_template_version(template, version="v2.0.0")


def test_template_version_default_region_uses_source_region():
    template = _template([_version("1", "v1.0.0", region="eu-west-1")])

    assert (
        _default_region_for_template_version(
            template.pod_template_versions_by_template_id.nodes[0]
        )
        == "eu-west-1"
    )


def test_template_target_regions_include_source_region_once():
    pod = SourcePodInfo.model_validate({
        "id": "1",
        "metadata": {},
        "cpuMillicores": 2000,
        "memoryBytes": 8 * 1024 * 1024 * 1024,
        "storageGigs": 20,
        "gpus": 0,
        "deployment": {"targetRegion": "us-east-1"},
    })

    assert _template_target_regions(pod, ("us-east-1", "eu-west-1")) == [
        "us-east-1",
        "eu-west-1",
    ]
