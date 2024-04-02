import re
from pathlib import Path

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
    pod_name = Path("/etc/hostname").read_text().strip()

    match = pod_name_regex.match(pod_name)
    if not match:
        raise RuntimeError(f"Invalid pod name: {pod_name}")

    token = match.group("token")
    node_name = match.group("node_name")
    retry = match.group("retry")
    arr_index = match.group("arr_index")
    arr_retry = match.group("arr_retry")

    execute(
        gql.gql("""
            mutation OverrideTaskStatus(
                $argToken: String!
                $argNodeName: String!,
                $argRetry: BigInt!,
                $argArrIndex: BigInt,
                $argArrRetry: BigInt,
                $argStatus: String!
            ) {
                overrideTaskStatusByToken(
                    input: {
                        argToken: $argToken,
                        argNodeName: $argNodeName,
                        argRetry: $argRetry,
                        argArrIndex: $argArrIndex,
                        argArrRetry: $argArrRetry,
                        argStatus: $argStatus
                    }
                ) {
                    clientMutationId
                }
            }
        """),
        {
            "argToken": token,
            "argNodeName": node_name,
            "argRetry": retry,
            "argArrIndex": arr_index,
            "argArrRetry": arr_retry,
            "argStatus": status,
        },
    )
