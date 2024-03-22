import re

import gql
from latch_sdk_gql.execute import execute

pod_name_regex = re.compile(
    r"^(?P<token>[a-zA-Z0-9]+)-(?P<node_name>(n\d+-\d+-)*n\d+)-(?P<retry>\d)+(?:-(?P<arr_index>\d+))?(?:-(?P<arr_retry>\d+))?$"
)


def _override_task_status(status: str) -> None:
    with open("/etc/hostname", "r") as f:
        pod_name = f.read().strip()

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
