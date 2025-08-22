import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

import click
from pkg_resources import get_distribution

from latch_cli.constants import latch_constants


class BaseImageOptions(str, Enum):
    default = "default"
    cuda = "cuda"
    opencl = "opencl"
    nextflow = "nextflow"


@dataclass(frozen=True)
class LatchWorkflowConfig:
    """Configuration for a Latch workflow."""

    latch_version: str
    """Version of the Latch SDK used to initialize the workflow"""
    base_image: str
    """Exact version of the included workflow base image"""
    date: str
    """Timestamp of the `latch init` call"""


def get_or_create_workflow_config(
    config_path: Path, base_image_type: BaseImageOptions = BaseImageOptions.default
) -> LatchWorkflowConfig:
    if config_path.exists() and config_path.is_file():
        try:
            return LatchWorkflowConfig(**json.loads(config_path.read_text()))
        except json.JSONDecodeError:
            click.secho(
                f"Unable to load config from {config_path}, regenerating", dim=True
            )

    base_image = latch_constants.base_image

    if base_image_type != BaseImageOptions.default:
        base_image = base_image.replace(
            "latch-base", f"latch-base-{base_image_type.name}"
        )

    if base_image_type == BaseImageOptions.nextflow:
        base_image = re.sub(
            r"([^:]+)$", latch_constants.nextflow_latest_version, base_image
        )

    config = LatchWorkflowConfig(
        latch_version=get_distribution("latch").version,
        base_image=base_image,
        date=datetime.now(timezone.utc).isoformat(),
    )

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(json.dumps(asdict(config)))

    return config
