import gql
from latch_sdk_gql.execute import execute

from latch.executions import get_task_identifier


def _override_task_status(status: str) -> None:
    task_id = get_task_identifier()
    if task_id is None:
        raise RuntimeError("Could not determine task identifier")

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
            "argToken": task_id.token,
            "argNodeName": task_id.node_name,
            "argRetry": task_id.retry,
            "argArrIndex": task_id.arr_index,
            "argArrRetry": task_id.arr_retry,
            "argStatus": status,
        },
    )
