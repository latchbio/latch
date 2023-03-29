from typing import Annotated, List, Optional

from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.launch_plan import reference_launch_plan

from ..types.directory import LatchDir
from ..types.file import LatchFile


@reference_launch_plan(
    project="4107",
    domain="development",
    name="wf.__init__.deseq2_wf",
    version="1.3.18-7a4e16",
)
def deseq2_wf(
    report_name: str,
    count_table_source: str = "single",
    raw_count_table: Optional[
        Annotated[
            LatchFile,
            FlyteAnnotation(
                {
                    "_tmp_hack_deseq2": "counts_table",
                    "rules": [
                        {
                            "regex": r".*\.(csv|tsv|xlsx)$",
                            "message": "Expected a CSV, TSV, or XLSX file",
                        }
                    ],
                }
            ),
        ]
    ] = None,
    raw_count_tables: List[LatchFile] = [],
    count_table_gene_id_column: str = "gene_id",
    output_location_type: str = "default",
    output_location: Optional[LatchDir] = None,
    conditions_source: str = "manual",
    manual_conditions: Annotated[
        List[List[str]],
        FlyteAnnotation({"_tmp_hack_deseq2": "manual_design_matrix"}),
    ] = [],
    conditions_table: Optional[
        Annotated[
            LatchFile,
            FlyteAnnotation(
                {
                    "_tmp_hack_deseq2": "design_matrix",
                    "rules": [
                        {
                            "regex": r".*\.(csv|tsv|xlsx)$",
                            "message": "Expected a CSV, TSV, or XLSX file",
                        }
                    ],
                }
            ),
        ]
    ] = None,
    design_matrix_sample_id_column: Optional[
        Annotated[str, FlyteAnnotation({"_tmp_hack_deseq2": "design_id_column"})]
    ] = None,
    design_formula: Annotated[
        List[List[str]],
        FlyteAnnotation(
            {
                "_tmp_hack_deseq2": "design_formula",
                "_tmp_hack_deseq2_allow_clustering": True,
            }
        ),
    ] = [],
    number_of_genes_to_plot: int = 30,
) -> LatchDir:
    ...
