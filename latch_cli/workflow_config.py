import json
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path

from pkg_resources import get_distribution

from latch_cli.constants import latch_constants


@dataclass(frozen=True)
class LatchWorkflowConfig:
    """Configuration for a Latch workflow."""

    latch_version: str
    """Version of the Latch SDK used to initialize the workflow"""
    base_image: str
    """Exact version of the included workflow base image"""
    date: str
    """Timestamp of the `latch init` call"""


def create_and_write_config(base_image: str, pkg_root: Path):
    config = LatchWorkflowConfig(
        latch_version=get_distribution("latch").version,
        base_image=base_image,
        date=datetime.now().isoformat(),
    )

    (pkg_root / ".latch").mkdir(exist_ok=True)

    with open(pkg_root / latch_constants.pkg_config, "w") as f:
        f.write(json.dumps(asdict(config)))
