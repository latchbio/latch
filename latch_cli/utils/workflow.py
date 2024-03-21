import gql
from latch_sdk_gql.execute import execute


def override_task_status(status: str) -> None:
    with open("/etc/hostname", "r") as f:
        pod_name = f.read().strip()

    s = pod_name.split("-")

    try:
        token = s[0]
        node_name = s[1]
        retry = s[2]
    except IndexError:
        raise RuntimeError(f"Invalid pod name: {pod_name}")

    arr_index = None if len(s) <= 3 else s[3]
    arr_retry = None if len(s) <= 4 else s[4]

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
