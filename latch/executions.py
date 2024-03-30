import os
import re
from dataclasses import dataclass
from typing import Optional

import click
import gql
from latch_sdk_gql.execute import execute

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
