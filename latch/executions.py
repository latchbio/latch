import os
import re
from dataclasses import dataclass
from typing import List, Optional

import click
import gql
from latch_sdk_gql.execute import execute

from latch_cli.utils.path import normalize_path

pod_name_regex = re.compile(
    r"""
    ^(
        (?P<token>[a-zA-Z0-9]+)
        -
        (?P<node_name>
            (
                n
                [0-9]+
                -
                [0-9]+
                -
            )*
            n
            [0-9]+
        )
        -
        (?P<retry>[0-9]+)
        (
            -
            (?P<arr_index>[0-9]+)
        )?
        (
            -
            (?P<arr_retry>[0-9]+)
        )?
        (-preexec)?
    )$
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class TaskIdentifier:
    token: str
    node_name: str
    retry: int
    arr_index: Optional[int]
    arr_retry: Optional[int]


def get_task_identifier() -> Optional[TaskIdentifier]:
    try:
        with open("/etc/hostname", "r") as f:
            pod_name = f.read().strip()
    except FileNotFoundError:
        return None

    match = pod_name_regex.match(pod_name)
    if not match:
        return None

    return TaskIdentifier(
        token=match.group("token"),
        node_name=match.group("node_name"),
        retry=int(match.group("retry")),
        arr_index=(
            int(match.group("arr_index"))
            if match.group("arr_index") is not None
            else None
        ),
        arr_retry=(
            int(match.group("arr_retry"))
            if match.group("arr_retry") is not None
            else None
        ),
    )


def rename_current_execution(name: str):
    """Rename the current execution.

    Useful for naming executions based on inputs. If this function is called
    outside of an execution context, it is a noop.
    """
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID", None)
    if token is None:
        # noop during local execution / testing
        click.secho(
            "Running in local execution context - skipping rename.",
            dim=True,
            italic=True,
        )
        return

    execute(
        gql.gql("""
            mutation RenameExecution($argName: String!, $argToken: String!) {
                renameExecutionByToken(input: {argToken: $argToken, argName: $argName}) {
                    clientMutationId
                }
            }
        """),
        {"argName": name, "argToken": token},
    )


def add_execution_results(results: List[str]):
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    workspace_id = os.environ.get("FLYTE_INTERNAL_EXECUTION_PROJECT")
    if token is None or workspace_id is None:
        # noop during local execution / testing
        click.secho(
            "Running in local execution context - skip adding output results.",
            dim=True,
            italic=True,
        )
        return

    results = [
        normalize_path(r, workspace=workspace_id, assume_remote=True) for r in results
    ]

    execute(
        gql.gql("""
            mutation addExecutionResults(
                $argToken: String!,
                $argPaths: [String]!
            ) {
                executionInfoMetadataPublishResults(
                    input: {argToken: $argToken, argPaths: $argPaths}
                ) {
                    clientMutationId
                }
            }
        """),
        {"argToken": token, "argPaths": results},
    )


def report_nextflow_used_storage(used_bytes: int):
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        return

    execute(
        gql.gql("""
            mutation updateNextflowStorageSize(
                $argToken: String!,
                $argUsedStorageBytes: BigInt!
            ) {
                nfExecutionInfoUpdateUsedStorageBytes(
                    input: {argToken: $argToken, argUsedStorageBytes: $argUsedStorageBytes}
                ) {
                    clientMutationId
                }
            }
        """),
        {"argToken": token, "argUsedStorageBytes": used_bytes},
    )
