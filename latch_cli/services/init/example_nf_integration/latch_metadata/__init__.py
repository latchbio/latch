from pathlib import Path

from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile
from latch.types.metadata import (
    LatchAuthor,
    NextflowFileParameter,
    NextflowMetadata,
    NextflowParameter,
)

NextflowMetadata(
    display_name="Test NF Workflow - FastQC",
    author=LatchAuthor(),
    parameters={
        "fastqDir": NextflowFileParameter(
            type=LatchDir,
            display_name="FastQ Directory",
            path=Path("/root/fastqs"),
            default=LatchDir("latch://1721.account/test-nf-fastqs"),
        ),
        "decompress_files": NextflowParameter(
            type=bool,
            display_name="Decompress Files",
        ),
    },
)
