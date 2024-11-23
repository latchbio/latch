from collections.abc import Iterable
from dataclasses import dataclass
from typing import Optional

import flyteidl.admin.launch_plan_pb2 as pb
import google.protobuf.wrappers_pb2 as pb_wrap

from ..core.execution import QualityOfService
from ..core.identifier import Identifier
from ..core.interface import ParameterMap
from ..core.literals import LiteralMap
from ..core.security import SecurityContext
from ..utils import to_idl_many, try_to_idl
from .common import Annotations, AuthRole, Labels, Notification, RawOutputDataConfig
from .schedule import Schedule


@dataclass
class Auth:
    """
    Defines permissions associated with executions created by this launch plan spec.
    Use either of these roles when they have permissions required by your workflow execution.
    Deprecated.
    """

    assumable_iam_role: str
    """Defines an optional iam role which will be used for tasks run in executions created with this launch plan."""

    kubernetes_service_account: str
    """Defines an optional kubernetes service account which will be used for tasks run in executions created with this launch plan."""

    def to_idl(self) -> pb.Auth:
        return pb.Auth(
            assumable_iam_role=self.assumable_iam_role,
            kubernetes_service_account=self.kubernetes_service_account,
        )


@dataclass
class LaunchPlanSpec:
    """User-provided launch plan definition and configuration values."""

    workflow_id: Identifier
    """Reference to the Workflow template that the launch plan references"""

    entity_metadata: "LaunchPlanMetadata"
    """Metadata for the Launch Plan"""

    default_inputs: ParameterMap
    """
    Input values to be passed for the execution.
    These can be overriden when an execution is created with this launch plan.
    """

    fixed_inputs: LiteralMap
    """
    Fixed, non-overridable inputs for the Launch Plan.
    These can not be overriden when an execution is created with this launch plan.
    """

    """
    String to indicate the role to use to execute the workflow underneath

    Deprecated
    """
    role: str

    labels: Labels
    """Custom labels to be applied to the execution resource."""

    annotations: Annotations
    """Custom annotations to be applied to the execution resource."""

    security_context: SecurityContext
    """Indicates security context for permissions triggered with this launch plan"""

    quality_of_service: QualityOfService
    """Indicates the runtime priority of the execution."""

    raw_output_data_config: RawOutputDataConfig
    """Encapsulates user settings pertaining to offloaded data (i.e. Blobs, Schema, query data, etc.)."""

    max_parallelism: int
    """
    Controls the maximum number of tasknodes that can be run in parallel for the entire workflow.
    This is useful to achieve fairness. Note: MapTasks are regarded as one unit,
    and parallelism/concurrency of MapTasks is independent from this.
    """

    interruptible: Optional[bool] = None
    """
    Allows for the interruptible flag of a workflow to be overwritten for a single execution.
    Omitting this field uses the workflow's value as a default.
    As we need to distinguish between the field not being provided and its default value false, we have to use a wrapper
    around the bool field.
    """

    auth: Optional[Auth] = None
    """
    Indicates the permission associated with workflow executions triggered with this launch plan.

    Deprecated
    """

    auth_role: Optional[AuthRole] = None

    def to_idl(self) -> pb.LaunchPlanSpec:
        return pb.LaunchPlanSpec(
            workflow_id=self.workflow_id.to_idl(),
            entity_metadata=self.entity_metadata.to_idl(),
            default_inputs=self.default_inputs.to_idl(),
            fixed_inputs=self.fixed_inputs.to_idl(),
            role=self.role,
            labels=self.labels.to_idl(),
            annotations=self.annotations.to_idl(),
            auth=try_to_idl(self.auth),
            auth_role=try_to_idl(self.auth_role),
            security_context=self.security_context.to_idl(),
            quality_of_service=self.quality_of_service.to_idl(),
            raw_output_data_config=self.raw_output_data_config.to_idl(),
            max_parallelism=self.max_parallelism,
            interruptible=pb_wrap.BoolValue(value=self.interruptible),
        )


@dataclass
class LaunchPlanMetadata:
    """
    Additional launch plan attributes included in the LaunchPlanSpec not strictly required to launch
    the reference workflow.
    """

    schedule: Schedule
    """Schedule to execute the Launch Plan"""

    notifications: Iterable[Notification]
    """List of notifications based on Execution status transitions"""

    def to_idl(self) -> pb.LaunchPlanMetadata:
        return pb.LaunchPlanMetadata(
            schedule=self.schedule.to_idl(),
            notifications=to_idl_many(self.notifications),
        )
