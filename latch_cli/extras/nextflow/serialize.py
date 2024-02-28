from latch_cli.extras.common.serialize import serialize
from latch_cli.extras.nextflow.workflow import NextflowWorkflow


def serialize_nf(
    nf_wf: NextflowWorkflow,
    output_dir: str,
    image_name: str,
    dkr_repo: str,
):
    serialize(nf_wf, output_dir, image_name, dkr_repo, write_spec=True)
