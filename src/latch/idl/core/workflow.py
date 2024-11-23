import typing
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import timedelta
from enum import Enum
from typing import Optional

import flyteidl.core.workflow_pb2 as pb

from ..utils import dur_from_td, to_idl_many, try_to_idl
from .condition import BooleanExpression
from .execution import QualityOfService, QualityOfServiceDesignationTier
from .identifier import Identifier
from .interface import TypedInterface
from .literals import Binding, RetryStrategy
from .tasks import Resources
from .types import Error


@dataclass
class IfBlock:
    """Defines a condition and the execution unit that should be executed if the condition is satisfied."""

    condition: BooleanExpression
    then_node: "Node"

    def to_idl(self) -> pb.IfBlock:
        return pb.IfBlock(
            condition=self.condition.to_idl(), then_node=self.then_node.to_idl()
        )


@dataclass
class IfElseBlock:
    """
    Defines a series of if/else blocks. The first branch whose condition evaluates to true is the one to execute.
    If no conditions were satisfied, the else_node or the error will execute.
    """

    case: IfBlock
    """+required. First condition to evaluate."""

    other: Iterable[IfBlock]
    """+optional. Additional branches to evaluate."""

    default: "typing.Union[IfElseBlockElseNode, IfElseBlockError]"
    """+required."""

    def to_idl(self) -> pb.IfElseBlock:
        res = pb.IfElseBlock(
            case=self.case.to_idl(), other=(x.to_idl() for x in self.other)
        )
        res.MergeFrom(self.default.to_idl())
        return res


@dataclass
class IfElseBlockElseNode:
    """Execute a node in case none of the branches were taken."""

    else_node: "Node"

    def to_idl(self) -> pb.IfElseBlock:
        return pb.IfElseBlock(else_node=self.else_node.to_idl())


@dataclass
class IfElseBlockError:
    """Throw an error in case none of the branches were taken."""

    error: Error

    def to_idl(self) -> pb.IfElseBlock:
        return pb.IfElseBlock(error=self.error.to_idl())


@dataclass
class BranchNode:
    """
    BranchNode is a special node that alter the flow of the workflow graph. It allows the control flow to branch at
    runtime based on a series of conditions that get evaluated on various parameters (e.g. inputs, primitives).
    """

    if_else: IfElseBlock
    """+required"""

    def to_idl(self) -> pb.BranchNode:
        return pb.BranchNode(if_else=self.if_else.to_idl())


@dataclass
class TaskNode:
    """Refers to the task that the Node is to execute."""

    reference: "TaskNodeReferenceId"  # oneof with one element

    overrides: "TaskNodeOverrides"
    """Optional overrides applied at task execution time."""

    def to_idl(self) -> pb.TaskNode:
        res = pb.TaskNode(overrides=self.overrides.to_idl())
        res.MergeFrom(self.reference.to_idl())
        return res


@dataclass
class TaskNodeReferenceId:
    """Use a globally unique identifier for the task."""

    reference_id: Identifier

    def to_idl(self) -> pb.TaskNode:
        return pb.TaskNode(reference_id=self.reference_id.to_idl())


@dataclass
class WorkflowNode:
    """Refers to a the workflow the node is to execute."""

    reference: "typing.Union[WorkflowNodeLaunchplanRef, WorkflowNodeSubWorkflowRef]"

    def to_idl(self) -> pb.WorkflowNode:
        return self.reference.to_idl()


@dataclass
class WorkflowNodeLaunchplanRef:
    """Use a launch plan with a globally unique identifier."""

    launchplan_ref: Identifier

    def to_idl(self) -> pb.WorkflowNode:
        return pb.WorkflowNode(launchplan_ref=self.launchplan_ref.to_idl())


@dataclass
class WorkflowNodeSubWorkflowRef:
    """Reference a subworkflow, that should be defined with the compiler context"""

    sub_workflow_ref: Identifier

    def to_idl(self) -> pb.WorkflowNode:
        return pb.WorkflowNode(sub_workflow_ref=self.sub_workflow_ref.to_idl())


@dataclass
class NodeMetadata:
    """Defines extra information about the Node."""

    name: str
    """A friendly name for the Node"""

    timeout: timedelta
    """The overall timeout of a task."""

    retries: RetryStrategy
    """Number of retries per task."""

    interruptible_value: "NodeMetadataInterruptible"  # oneof with one element

    def to_idl(self) -> pb.NodeMetadata:
        res = pb.NodeMetadata(
            name=self.name,
            timeout=dur_from_td(self.timeout),
            retries=self.retries.to_idl(),
        )
        res.MergeFrom(self.interruptible_value.to_idl())
        return res


@dataclass
class NodeMetadataInterruptible:
    """Identify whether node is interruptible"""

    interruptible: bool

    def to_idl(self) -> pb.NodeMetadata:
        return pb.NodeMetadata(interruptible=self.interruptible)


@dataclass
class Alias:
    """Links a variable to an alias."""

    var: str
    """Must match one of the output variable names on a node."""

    alias: str
    """A workflow-level unique alias that downstream nodes can refer to in their input."""

    def to_idl(self) -> pb.Alias:
        return pb.Alias(var=self.var, alias=self.alias)


@dataclass
class Node:
    """
    A Workflow graph Node. One unit of execution in the graph. Each node can be linked to a Task, a Workflow or a branch
    node.
    """

    id: str
    """
    A workflow-level unique identifier that identifies this node in the workflow. "inputs" and "outputs" are reserved
    node ids that cannot be used by other nodes.
    """

    metadata: NodeMetadata
    """Extra metadata about the node."""

    inputs: Iterable[Binding]
    """
    Specifies how to bind the underlying interface's inputs. All required inputs specified in the underlying interface
    must be fulfilled.
    """

    target: "typing.Union[NodeTargetTask, NodeTargetWorkflow, NodeTargetBranch]"
    """Information about the target to execute in this node."""

    upstream_node_ids: Iterable[str] = field(default_factory=list)
    """
    +optional Specifies execution dependency for this node ensuring it will only get scheduled to run after all its
    upstream nodes have completed. This node will have an implicit dependency on any node that appears in inputs
    field.
    """

    output_aliases: Iterable[Alias] = field(default_factory=list)
    """
    +optional. A node can define aliases for a subset of its outputs. This is particularly useful if different nodes
    need to conform to the same interface (e.g. all branches in a branch node). Downstream nodes must refer to this
    nodes outputs using the alias if one's specified.
    """

    def to_idl(self) -> pb.Node:
        res = pb.Node(
            id=self.id,
            metadata=self.metadata.to_idl(),
            inputs=(x.to_idl() for x in self.inputs),
            upstream_node_ids=self.upstream_node_ids,
            output_aliases=(x.to_idl() for x in self.output_aliases),
        )

        res.MergeFrom(self.target.to_idl())

        return res


@dataclass
class NodeTargetTask:
    """Information about the Task to execute in this node."""

    task_node: TaskNode

    def to_idl(self) -> pb.Node:
        return pb.Node(task_node=self.task_node.to_idl())


@dataclass
class NodeTargetWorkflow:
    """Information about the Workflow to execute in this mode."""

    workflow_node: WorkflowNode

    def to_idl(self) -> pb.Node:
        return pb.Node(workflow_node=self.workflow_node.to_idl())


@dataclass
class NodeTargetBranch:
    """Information about the Workflow to execute in this mode."""

    branch_node: BranchNode

    def to_idl(self) -> pb.Node:
        return pb.Node(branch_node=self.branch_node.to_idl())


@dataclass
class WorkflowMetadata:
    """
    This is workflow layer metadata. These settings are only applicable to the workflow as a whole, and do not
    percolate down to child entities (like tasks) launched by the workflow.
    """

    quality_of_service: QualityOfService = field(default_factory=QualityOfService)
    """Indicates the runtime priority of workflow executions."""

    class OnFailurePolicy(int, Enum):
        """Failure Handling Strategy"""

        fail_immediately = pb.WorkflowMetadata.FAIL_IMMEDIATELY
        """
        FAIL_IMMEDIATELY instructs the system to fail as soon as a node fails in the workflow. It'll automatically
        abort all currently running nodes and clean up resources before finally marking the workflow executions as
        failed.
        """

        fail_after_executable_nodes_complete = (
            pb.WorkflowMetadata.FAIL_AFTER_EXECUTABLE_NODES_COMPLETE
        )
        """
        FAIL_AFTER_EXECUTABLE_NODES_COMPLETE instructs the system to make as much progress as it can. The system will
        not alter the dependencies of the execution graph so any node that depend on the failed node will not be run.
        Other nodes that will be executed to completion before cleaning up resources and marking the workflow
        execution as failed.
        """

        def to_idl(self) -> pb.WorkflowMetadata.OnFailurePolicy:
            return self.value

    on_failure: OnFailurePolicy = OnFailurePolicy.fail_immediately
    """Defines how the system should behave when a failure is detected in the workflow execution."""

    def to_idl(self) -> pb.WorkflowMetadata:
        return pb.WorkflowMetadata(
            quality_of_service=self.quality_of_service.to_idl(),
            on_failure=self.on_failure.to_idl(),
        )


@dataclass
class WorkflowMetadataDefaults:
    """
    The difference between these settings and the WorkflowMetadata ones is that these are meant to be passed down to
    a workflow's underlying entities (like tasks). For instance, 'interruptible' has no meaning at the workflow layer, it
    is only relevant when a task executes. The settings here are the defaults that are passed to all nodes
    unless explicitly overridden at the node layer.
    If you are adding a setting that applies to both the Workflow itself, and everything underneath it, it should be
    added to both this object and the WorkflowMetadata object above.
    """

    interruptible: bool = False
    """Whether child nodes of the workflow are interruptible."""

    def to_idl(self) -> pb.WorkflowMetadataDefaults:
        return pb.WorkflowMetadataDefaults(interruptible=self.interruptible)


@dataclass
class WorkflowTemplate:
    """
    Flyte Workflow Structure that encapsulates task, branch and subworkflow nodes to form a statically analyzable,
    directed acyclic graph.
    """

    id: Identifier
    """A globally unique identifier for the workflow."""

    interface: TypedInterface
    """Defines a strongly typed interface for the Workflow. This can include some optional parameters."""

    nodes: Iterable[Node]
    """A list of nodes. In addition, "globals" is a special reserved node id that can be used to consume workflow inputs."""

    outputs: Iterable[Binding]
    """
    A list of output bindings that specify how to construct workflow outputs. Bindings can pull node outputs or
    specify literals. All workflow outputs specified in the interface field must be bound in order for the workflow
    to be validated. A workflow has an implicit dependency on all of its nodes to execute successfully in order to
    bind final outputs.
    Most of these outputs will be Binding's with a BindingData of type OutputReference.  That is, your workflow can
    just have an output of some constant (`Output(5)`), but usually, the workflow will be pulling
    outputs from the output of a task.
    """

    metadata_defaults: WorkflowMetadataDefaults = field(
        default_factory=WorkflowMetadataDefaults
    )
    """workflow defaults"""

    metadata: WorkflowMetadata = field(default_factory=WorkflowMetadata)
    """Extra metadata about the workflow."""

    failure_node: Optional[Node] = None
    """
    +optional A catch-all node. This node is executed whenever the execution engine determines the workflow has failed.
    The interface of this node must match the Workflow interface with an additional input named "error" of type
    pb.lyft.flyte.core.Error.
    """

    def to_idl(self) -> pb.WorkflowTemplate:
        return pb.WorkflowTemplate(
            id=self.id.to_idl(),
            metadata=self.metadata.to_idl(),
            interface=self.interface.to_idl(),
            nodes=to_idl_many(self.nodes),
            outputs=to_idl_many(self.outputs),
            failure_node=try_to_idl(self.failure_node),
            metadata_defaults=self.metadata_defaults.to_idl(),
        )


@dataclass
class TaskNodeOverrides:
    """Optional task node overrides that will be applied at task execution time."""

    resources: Resources
    """A customizable interface to convey resources requested for a task container. """

    def to_idl(self) -> pb.TaskNodeOverrides:
        return pb.TaskNodeOverrides(resources=self.resources.to_idl())
