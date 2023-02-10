import datetime
from dataclasses import dataclass

from dataclasses_json import dataclass_json


@dataclass_json
@dataclass
class LatchWorkflowConfig:
    """Configuration for a Latch workflow."""

    latch_version: str
    """Latch version used to initialize the workflow."""
    base_image: str
    """Base image at initialization time."""
    date: datetime.datetime
    """Date and time of initialization"""
