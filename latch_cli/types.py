from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class LatchWorkflowConfig:
    """Configuration for a Latch workflow."""

    latch_version: str
    """Version of the Latch SDK used to initialize the workflow"""
    base_image: str
    """Exact version of the included workflow base image"""
    date: str
    """Time stamp of the `latch init` call"""
