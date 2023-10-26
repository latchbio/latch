from dataclasses import dataclass
from enum import Enum

import flyteidl.core.identifier_pb2 as pb


class ResourceType(int, Enum):
    unspecified = pb.UNSPECIFIED
    task = pb.TASK
    workflow = pb.WORKFLOW
    launch_plan = pb.LAUNCH_PLAN
    dataset = pb.DATASET
    """
    A dataset represents an entity modeled in Flyte DataCatalog. A Dataset is also a versioned entity and can be a compilation of multiple individual objects.
    Eventually all Catalog objects should be modeled similar to Flyte Objects. The Dataset entities makes it possible for the UI  and CLI to act on the objects
    in a similar manner to other Flyte objects
    """

    def to_idl(self) -> pb.ResourceType:
        return self.value


@dataclass
class Identifier:
    """Encapsulation of fields that uniquely identifies a Flyte resource."""

    resource_type: ResourceType
    """Identifies the specific type of resource that this identifier corresponds to."""

    project: str
    """Name of the project the resource belongs to."""
    domain: str
    """
    Name of the domain the resource belongs to.
    A domain can be considered as a subset within a specific project.
    """
    name: str
    """User provided value for the resource."""
    version: str
    """Specific version of the resource."""

    def to_idl(self) -> pb.Identifier:
        return pb.Identifier(
            resource_type=self.resource_type.to_idl(),
            project=self.project,
            domain=self.domain,
            name=self.name,
            version=self.version,
        )


@dataclass
class WorkflowExecutionIdentifier:
    """Encapsulation of fields that uniquely identifies a Flyte workflow execution"""

    project: str
    """Name of the project the resource belongs to."""
    domain: str
    """
    Name of the domain the resource belongs to.
    A domain can be considered as a subset within a specific project.
    """
    name: str
    """User provided value for the resource."""

    def to_idl(self) -> pb.WorkflowExecutionIdentifier:
        return pb.WorkflowExecutionIdentifier(
            project=self.project,
            domain=self.domain,
            name=self.name,
        )


@dataclass
class NodeExecutionIdentifier:
    """Encapsulation of fields that identify a Flyte node execution entity."""

    node_id: str
    execution_id: WorkflowExecutionIdentifier

    def to_idl(self) -> pb.NodeExecutionIdentifier:
        return pb.NodeExecutionIdentifier(
            node_id=self.node_id, execution_id=self.execution_id.to_idl()
        )


@dataclass
class TaskExecutionIdentifier:
    """Encapsulation of fields that identify a Flyte task execution entity."""

    task_id: Identifier
    node_execution_id: NodeExecutionIdentifier
    retry_attempt: int = 0

    def to_idl(self) -> pb.TaskExecutionIdentifier:
        return pb.TaskExecutionIdentifier(
            task_id=self.task_id.to_idl(),
            node_execution_id=self.node_execution_id.to_idl(),
            retry_attempt=self.retry_attempt,
        )
