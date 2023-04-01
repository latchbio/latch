from enum import Enum
from typing import Optional

from flytekit.core.launch_plan import reference_launch_plan

from ..types.directory import LatchDir
from ..types.file import LatchFile


class BaseQualityEncoding(Enum):
    phred33 = "--phred33"
    phred64 = "--phred64"


class AdapterSequence(Enum):
    auto = "auto"
    illumina = "--illumina"
    stranded_illumina = "--stranded_illumina"
    nextera = "--nextera"
    small_rna = "--small_rna"


@reference_launch_plan(
    project="1656",
    domain="development",
    name="wf.__init__.trim_galore",
    version="0.0.12-bcecec",
)
def trim_galore(
    input_forward: LatchFile,
    input_reverse: LatchFile,
    base_out: Optional[str],
    output_directory: Optional[LatchDir],
    fastqc_args: Optional[str],
    adapter: Optional[str],
    adapter2: Optional[str],
    consider_already_trimmed: Optional[int],
    max_length: Optional[int],
    max_n: Optional[float],
    clip_R1: Optional[int],
    clip_R2: Optional[int],
    three_prime_clip_R1: Optional[int],
    three_prime_clip_R2: Optional[int],
    hardtrim5: Optional[int],
    hardtrim3: Optional[int],
    quality: int = 20,
    base_quality_encoding: BaseQualityEncoding = BaseQualityEncoding.phred33,
    fastqc: bool = True,
    adapter_sequence: AdapterSequence = AdapterSequence.auto,
    stringency: int = 1,
    error_rate: float = 0.01,
    gzip_output_files: bool = False,
    length: int = 20,
    trim_n: bool = False,
    report_file: bool = True,
    polyA: bool = False,
    implicon: bool = False,
    retain_unpaired: bool = True,
    length_1: int = 35,
    length_2: int = 35,
) -> LatchDir:
    ...
