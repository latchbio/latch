from typing import cast

import gql
from latch_sdk_gql.execute import execute

from latch_cli.extras.common.serialize import serialize
from latch_cli.extras.nextflow.tasks.base import NextflowBaseTask
from latch_cli.extras.nextflow.workflow import NextflowWorkflow
from latch_cli.utils import current_workspace


def serialize_nf(
    nf_wf: NextflowWorkflow,
    output_dir: str,
    image_name: str,
    dkr_repo: str,
):
    serialize(nf_wf, output_dir, image_name, dkr_repo)


def add_task_metadata(wf: NextflowWorkflow):
    node_names = []
    task_types = []

    for node in wf.nodes:
        task = cast(NextflowBaseTask, node.flyte_entity)

        node_names.append(node.id)
        task_types.append(task.nf_task_type)

    execute(
        gql.gql("""
            mutation NFBulkInsertMut(
                $nodeNames: [String]!
                $ownerId: BigInt!
                $types: [String]!
                $version: String!
                $workflowName: String!
            ) {
                bulkInsertNextflowNodeInfo(
                    input: {
                        ownerId: $ownerId
                        workflowName: $workflowName
                        version: $version
                        nodeNames: $nodeNames
                        types: $types
                    }
                ) {
                    clientMutationId
                }
            }
        """),
        {
            "ownerId": current_workspace(),
            "version": wf.version,
            "workflowName": wf.name,
            "nodeNames": node_names,
            "types": task_types,
        },
    )
