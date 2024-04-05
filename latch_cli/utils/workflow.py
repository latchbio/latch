import re

import gql
from latch_sdk_gql.execute import execute

from latch.executions import get_task_identifier

pod_name_regex = re.compile(
    r"""
    ^(
        (?P<token>[a-zA-Z0-9]+)
        -
        (?P<node_name>
            (
                n
                [^\-]+
                -
                [0-9]+
                -
            )*
            n
            [^\-]+
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
    )$
    """,
    re.VERBOSE,
)


def _override_task_status(status: str) -> None:
    task_id = get_task_identifier()

    execute(
        gql.gql("""
            mutation OverrideTaskStatus(
                $argToken: String!
                $argNodeName: String!,
                $argRetry: BigInt!,
                $argArrIndex: BigInt,
                $argStatus: String!
            ) {
                overrideTaskStatusByToken(
                    input: {
                        argToken: $argToken,
                        argNodeName: $argNodeName,
                        argRetry: $argRetry,
                        argArrIndex: $argArrIndex,
                        argStatus: $argStatus
                    }
                ) {
                    clientMutationId
                }
            }
        """),
        {
            "argToken": task_id.token,
            "argNodeName": task_id.node_name,
            "argRetry": task_id.retry,
            "argArrIndex": task_id.arr_index,
            "argStatus": status,
        },
    )
