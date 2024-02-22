import os

import click
import gql
from latch_sdk_gql.execute import execute


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
