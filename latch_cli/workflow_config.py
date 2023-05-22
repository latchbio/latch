import json
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from pkg_resources import get_distribution

from latch_cli.constants import latch_constants


class BaseImageOptions(str, Enum):
    default = "default"
    cuda = "cuda"
    opencl = "opencl"


@dataclass(frozen=True)
class LatchWorkflowConfig:
    """Configuration for a Latch workflow."""

    latch_version: str
    """Version of the Latch SDK used to initialize the workflow"""
    base_image: str
    """Exact version of the included workflow base image"""
    date: str
    """Timestamp of the `latch init` call"""


def create_and_write_config(
    pkg_root: Path, base_image_type: BaseImageOptions = BaseImageOptions.default
):
    base_image = latch_constants.base_image

    if base_image_type != BaseImageOptions.default:
        base_image = base_image.replace(
            "latch-base", f"latch-base-{base_image_type.name}"
        )

    config = LatchWorkflowConfig(
        latch_version=get_distribution("latch").version,
        base_image=base_image,
        date=datetime.now().isoformat(),
    )

    (pkg_root / ".latch").mkdir(exist_ok=True)

    with (pkg_root / latch_constants.pkg_config).open("w") as f:
        f.write(json.dumps(asdict(config)))
