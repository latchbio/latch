from flytekit.core.launch_plan import reference_launch_plan

from ..types.directory import LatchDir
from ..types.file import LatchFile


@reference_launch_plan(
    project="1",
    domain="development",
    name="wf.__init__.gene_ontology_pathway_analysis",
    version="0.0.43",
)
def gene_ontology_pathway_analysis(
    contrast_csv: LatchFile,
    report_name: str,
    number_of_pathways: int = 20,
    output_location: LatchDir = LatchDir("latch:///Pathway Analysis/"),
) -> LatchDir:
    ...
