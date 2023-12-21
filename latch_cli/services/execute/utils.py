from copy import deepcopy
from typing import List, Optional, TypedDict

import click
import gql
import graphql
from latch_sdk_config.user import user_config
from latch_sdk_gql.execute import execute

from latch_cli.click_utils import bold
from latch_cli.menus import select_tui
from latch_cli.utils import current_workspace


# todo(ayush): put this into latch_sdk_gql
def with_fragments(doc: graphql.DocumentNode, *fragments: graphql.DocumentNode):
    res = deepcopy(doc)

    defs = [*res.definitions]
    for fragment in fragments:
        defs.extend(fragment.definitions)

    res.definitions = tuple(defs)

    return res


fragments = (
    gql.gql("""
        fragment EGNFrag on ExecutionGraphNode {
            id
            status
            finalWorkflowGraphNode {
                nodeName
                taskInfo {
                    displayName
                }
            }
            taskExecutionInfos {
                nodes {
                    retry
                }
            }
            containerInfo(
                orderBy: INDEX_ASC
                filter: { status: { equalTo: RUNNING } }
            ) {
                nodes {
                    containerName
                    index
                }
            }
        }
    """),
    gql.gql("""
        fragment EIFrag on ExecutionInfo {
            id
            status
            displayName
            workflow {
                displayName
            }
            executionGraphNodesByExecutionId(condition: { status: RUNNING }) {
                nodes {
                    id
                    ...EGNFrag
                }
            }
        }
    """),
)


class ContainerNode(TypedDict):
    containerName: str
    index: str


class ContainerInfo(TypedDict):
    nodes: List[ContainerNode]


class TaskInfo(TypedDict):
    displayName: str


class FinalWGN(TypedDict):
    nodeName: str
    taskInfo: TaskInfo


class TaskExecutionNode(TypedDict):
    retry: str


class TaskExecutionInfos(TypedDict):
    nodes: List[TaskExecutionNode]


class EGNNode(TypedDict):
    id: str
    finalWorkflowGraphNode: FinalWGN
    taskExecutionInfos: TaskExecutionInfos
    containerInfo: ContainerInfo
    status: str


class EGNById(TypedDict):
    nodes: List[EGNNode]


class Workflow(TypedDict):
    displayName: str


class ExecutionInfoNode(TypedDict):
    id: str
    status: str
    displayName: str
    workflow: Workflow
    executionGraphNodesByExecutionId: EGNById


class RunningExecutions(TypedDict):
    nodes: List[ExecutionInfoNode]


def get_execution_info(execution_id: Optional[str]) -> ExecutionInfoNode:
    if execution_id is not None:
        info: Optional[ExecutionInfoNode] = execute(
            with_fragments(
                gql.gql("""
                    query ExecutionInfo($executionId: BigInt!) {
                        executionInfo(id: $executionId) {
                            id
                            ...EIFrag
                        }
                    }
                """),
                *fragments,
            ),
            {"executionId": execution_id},
        )["executionInfo"]

        if info is None:
            click.secho(
                f"Could not find an execution with ID {execution_id}.", fg="red"
            )
            raise click.exceptions.Exit(1)

        if info["status"] != "RUNNING":
            click.secho(
                f"The selected execution ({info['displayName']}) is no longer running.",
                fg="red",
            )
            raise click.exceptions.Exit(1)

        return info

    res: RunningExecutions = execute(
        with_fragments(
            gql.gql("""
                query RunningExecutions($createdBy: BigInt!) {
                    runningExecutions(argWorkspaceId: $createdBy) {
                        nodes {
                            id
                            ...EIFrag
                        }
                    }
                }
            """),
            *fragments,
        ),
        {"createdBy": current_workspace()},
    )["runningExecutions"]

    if len(res["nodes"]) == 0:
        click.secho("You have no executions currently running.", dim=True)
        raise click.exceptions.Exit(0)

    if len(res["nodes"]) == 1:
        execution = res["nodes"][0]
        click.secho(
            "Selecting execution"
            f" {click.style(execution['displayName'], bold=True, fg='blue')} as it is"
            " the only"
            " one running in Workspace"
            f" {click.style(user_config.workspace_name or user_config.workspace_id, bold=True, fg='blue')}.",
        )

        return execution

    selected_execution = select_tui(
        "You have multiple executions running in this workspace"
        f" ({user_config.workspace_name}). Which execution would you like to inspect?",
        [
            {
                "display_name": f'{x["displayName"]} ({x["workflow"]["displayName"]})',
                "value": x,
            }
            for x in res["nodes"]
        ],
        clear_terminal=False,
    )
    if selected_execution is None:
        click.secho("No execution selected. Exiting.", dim=True)
        raise click.exceptions.Exit(0)

    return selected_execution


def get_egn_info(
    execution_info: Optional[ExecutionInfoNode], egn_id: Optional[str] = None
) -> EGNNode:
    if egn_id is not None:
        res: Optional[EGNNode] = execute(
            with_fragments(
                gql.gql("""
                    query EGNInfo($egnId: BigInt!) {
                        executionGraphNode(id: $egnId) {
                            id
                            ...EGNFrag
                        }
                    }
                """),
                *fragments,
            ),
            {"egnId": egn_id},
        )["executionGraphNode"]

        if res is None:
            click.secho(f"Could not find a task with ID {egn_id}.", fg="red")
            raise click.exceptions.Exit(1)

        if res["status"] != "RUNNING":
            click.secho(
                "The selected task"
                f" ({res['finalWorkflowGraphNode']['taskInfo']['displayName']}) is no"
                " longer running.",
                fg="red",
            )
            raise click.exceptions.Exit(1)

        return res

    if execution_info is None:
        click.secho(
            "Aborting as neither an execution or task id were provided.", fg="red"
        )
        raise click.exceptions.Exit(1)

    egn_nodes = execution_info["executionGraphNodesByExecutionId"]["nodes"]

    if len(egn_nodes) == 0:
        click.secho(
            "No running tasks found for this execution"
            f" ({execution_info['displayName']}).",
            fg="red",
        )
        raise click.exceptions.Exit(1)

    if len(egn_nodes) == 1:
        node = egn_nodes[0]
        click.secho(
            "Selecting task"
            f" {click.style(node['finalWorkflowGraphNode']['taskInfo']['displayName'], bold=True, fg='blue')} as"
            " it is the only one running in Execution"
            f" {click.style(execution_info['displayName'], bold=True, fg='blue')}.",
        )

        return egn_nodes[0]

    selected_egn_node = select_tui(
        f"The execution you selected ({execution_info['displayName']}) has multiple"
        " running tasks. Which would you like to inspect?",
        [
            {
                "display_name": (
                    f"{x['finalWorkflowGraphNode']['taskInfo']['displayName']}"
                ),
                "value": x,
            }
            for x in egn_nodes
        ],
        clear_terminal=False,
    )

    if selected_egn_node is None:
        click.secho("No task selected.", dim=True)
        raise click.exceptions.Exit(0)

    return selected_egn_node


def get_container_info(
    egn_info: EGNNode, container_index: Optional[int] = None
) -> Optional[ContainerNode]:
    container_infos = egn_info["containerInfo"]["nodes"]

    if container_index is not None:
        for container in container_infos:
            if int(container["index"]) != container_index:
                continue

            return container

        click.secho(
            f"The specified container index ({container_index}) is either not present"
            " in this map task"
            f" ({egn_info['finalWorkflowGraphNode']['taskInfo']['displayName']}) or is"
            " no longer running."
        )
        raise click.exceptions.Exit(1)

    if len(container_infos) == 0:
        return None

    if len(container_infos) == 1:
        container = container_infos[0]
        click.echo(
            "Selecting container"
            f" {click.style(container['index'], bold=True, fg='blue')} as it is the"
            " only running"
            " container in Map Task"
            f" {click.style(egn_info['finalWorkflowGraphNode']['taskInfo']['displayName'], bold=True, fg='blue')}"
        )
        return container_infos[0]

    selected_container_info = select_tui(
        "You selected a Map Task with multiple running containers. Which one would you"
        " like to inspect?",
        [
            {"display_name": f'Container {x["index"]}', "value": x}
            for x in container_infos
        ],
        clear_terminal=False,
    )

    if selected_container_info is None:
        click.secho("No container selected.", dim=True)
        raise click.exceptions.Exit(0)

    return selected_container_info
