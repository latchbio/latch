from pathlib import Path

from latch.types import LatchDir
from latch.types.metadata import LatchAuthor, SnakemakeFileParameter, SnakemakeMetadata

SnakemakeMetadata(
    output_dir=LatchDir("latch:///sample_output"),
    display_name="snakemake_tutorial_workflow",
    author=LatchAuthor(
        name="Kenneth",
    ),
    parameters={
        "samples": SnakemakeFileParameter(
            display_name="Sample Input Directory",
            description="A directory full of FastQ files",
            type=LatchDir,
            path=Path("data/samples"),
        ),
        "ref_genome": SnakemakeFileParameter(
            display_name="Indexed Reference Genome",
            description=(
                "A directory with a reference Fasta file and the 6 index files produced"
                " from `bwa index`"
            ),
            type=LatchDir,
            path=Path("genome"),
        ),
    },
)
