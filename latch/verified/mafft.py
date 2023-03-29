from enum import Enum
from typing import Optional

from flytekit.core.launch_plan import reference_launch_plan

from ..types.directory import LatchDir
from ..types.file import LatchFile


class AlignmentMode(Enum):
    linsi = "L-INS-i"
    fftns2 = "FFT-NS-2"
    auto = "auto"


@reference_launch_plan(
    project="1656",
    domain="development",
    name="wf.__init__.mafft",
    version="1.0.0-e1a4bb",
)
def mafft(
    output_directory: Optional[LatchDir],
    unaligned_seqs: LatchFile,
    alignment_mode: AlignmentMode = AlignmentMode.auto,
    gap_penalty: float = 1.53,
    offset: float = 0.0,
    maxiterate: int = 0,
    output_file: str = "aligned_mafft.fa",
) -> LatchFile:
    ...
