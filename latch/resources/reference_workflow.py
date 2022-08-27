from typing import List, Optional

from flytekit.core.launch_plan import reference_launch_plan

from latch_cli.utils import current_workspace


def workflow_reference(
    name: str,
    version: str,
):
    return reference_launch_plan(
        project=current_workspace(),
        domain="development",
        name=name,
        version=version,
    )

@reference_launch_plan(
    project="1",
    domain="development",
    name="wf.__init__.rnaseq",
    version="0.0.317-f53c0e",
)
def rnaseq(
    samples: List[Sample],
    alignment_quantification_tools: AlignmentTools,
    ta_ref_genome_fork: str,
    sa_ref_genome_fork: str,
    output_location_fork: str,
    run_name: str,
    latch_genome: LatchGenome,
    bams: List[List[LatchFile]],
    custom_gtf: Optional[LatchFile] = None,
    custom_ref_genome: Optional[LatchFile] = None,
    custom_ref_trans: Optional[LatchFile] = None,
    star_index: Optional[LatchFile] = None,
    salmon_index: Optional[LatchFile] = None,
    save_indices: bool = False,
    custom_output_dir: Optional[LatchDir] = None,
) -> List[LatchFile]:
