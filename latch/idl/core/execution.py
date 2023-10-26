import typing
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum

import flyteidl.core.execution_pb2 as pb

from ..utils import dur_from_td


@dataclass
class WorkflowExecution:
    """Indicates various phases of Workflow Execution"""

    class Phase(int, Enum):
        undefined = pb.WorkflowExecution.UNDEFINED
        queued = pb.WorkflowExecution.QUEUED
        running = pb.WorkflowExecution.RUNNING
        succeeding = pb.WorkflowExecution.SUCCEEDING
        succeeded = pb.WorkflowExecution.SUCCEEDED
        failing = pb.WorkflowExecution.FAILING
        failed = pb.WorkflowExecution.FAILED
        aborted = pb.WorkflowExecution.ABORTED
        timed_out = pb.WorkflowExecution.TIMED_OUT
        aborting = pb.WorkflowExecution.ABORTING

        def to_idl(self) -> pb.WorkflowExecution.Phase:
            return self.value


@dataclass
class QualityOfServiceSpec:
    """Represents customized execution run-time attributes."""

    queueing_budget: timedelta
    """Indicates how much queueing delay an execution can tolerate."""

    # Add future, user-configurable options here

    def to_idl(self) -> pb.QualityOfServiceSpec:
        return pb.QualityOfServiceSpec(dur_from_td(self.queueing_budget))


@dataclass
class QualityOfService:
    """Indicates the priority of an execution."""

    class Tier(int, Enum):
        undefined = pb.QualityOfService.UNDEFINED
        """Default: no quality of service specified."""

        high = pb.QualityOfService.HIGH
        medium = pb.QualityOfService.MEDIUM
        low = pb.QualityOfService.LOW

        def to_idl(self) -> pb.QualityOfService.Tier:
            return self.value

    designation: "typing.Union[QualityOfServiceDesignationTier, QualityOfServiceDesignationSpec]" = field(
        default_factory=lambda: QualityOfServiceDesignationTier()
    )

    def to_idl(self) -> pb.QualityOfService:
        return self.designation.to_idl()


@dataclass
class QualityOfServiceDesignationTier:
    tier: QualityOfService.Tier = QualityOfService.Tier.undefined

    def to_idl(self) -> pb.QualityOfService:
        return pb.QualityOfService(tier=self.tier.to_idl())


@dataclass
class QualityOfServiceDesignationSpec:
    spec: QualityOfServiceSpec

    def to_idl(self) -> pb.QualityOfService:
        return pb.QualityOfService(spec=self.spec.to_idl())
