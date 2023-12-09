import os

import gql
from latch_sdk_gql.execute import execute


def rename_current_execution(name: str):
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID", None)
    if token is None:
        return  # noop during local execution / testing

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
