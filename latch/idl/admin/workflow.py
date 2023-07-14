from collections.abc import Iterable
from dataclasses import dataclass, field

import flyteidl.admin.workflow_pb2 as pb

from ..core.workflow import WorkflowTemplate
from ..utils import to_idl_many


@dataclass
class WorkflowSpec:
    """Represents a structure that encapsulates the specification of the workflow."""

    template: WorkflowTemplate
    """Template of the task that encapsulates all the metadata of the workflow."""

    sub_workflows: Iterable[WorkflowTemplate] = field(default_factory=list)
    """
    Workflows that are embedded into other workflows need to be passed alongside the parent workflow to the
    propeller compiler (since the compiler doesn't have any knowledge of other workflows - ie, it doesn't reach out
    to Admin to see other registered workflows).  In fact, subworkflows do not even need to be registered.
    """

    def to_idl(self) -> pb.WorkflowSpec:
        return pb.WorkflowSpec(
            template=self.template.to_idl(),
            sub_workflows=to_idl_many(self.sub_workflows),
        )
