"""
test.test_launch
~~~

    -
"""

from tempfile import NamedTemporaryFile

from latch_cli.services.launch import launch

simple_plan = """from latch.types import LatchFile

params = {
    "_name": "wf.__init__.assemble_and_sort",
    "read1": LatchFile("latch:///read1"),
    "read2": LatchFile("latch:///read2"),
}"""

crispresso_plan = """from latch.types import LatchFile, LatchDir


params = {
    "_name": "wf.__init__.crispresso2_wf",
    "output_folder": LatchDir("latch:///CRISPResso2_output/"),
    "fastq_r1": LatchFile("s3://latch-public/welcome/CRISPResso2/nhej.r1.fastq.gz"),
    "fastq_r2": LatchFile("s3://latch-public/welcome/CRISPResso2/nhej.r2.fastq.gz"),
    "amplicon_seq": [
        "AATGTCCCCCAATGGGAAGTTCATCTGGCACTGCCCACAGGTGAGGAGGTCATGATCCCCTTCTGGAGCTCCCAACGGGCCGTGGTCTGGTTCATCATCTGTAAGAATGGCTTCAAGAGGCTCGGCTGTGGTT"
    ],
    "name": "nhej",
}"""

rnaseq_plan = """from latch.types import LatchFile, LatchDir
from enum import Enum

class Strandedness(Enum):
    reverse = "reverse"
    forward = "forward"

params = {
    "_name": "wf.__init__.nf_rnaseq_wf",
    "sample_ids": [
        "WT_REP1",
        "RAP1_UNINDUCED_REP1",
        "RAP1_IAA_30M_REP1",
    ],
    "samples": [
        [
            LatchFile("s3://latch-public/welcome/nf_rnaseq/SRR6357070_1.fastq.gz"),
            LatchFile("s3://latch-public/welcome/nf_rnaseq/SRR6357070_2.fastq.gz"),
        ],
        [
            LatchFile("s3://latch-public/welcome/nf_rnaseq/SRR6357073_1.fastq.gz"),
        ],
        [
            LatchFile("s3://latch-public/welcome/nf_rnaseq/SRR6357076_1.fastq.gz"),
            LatchFile("s3://latch-public/welcome/nf_rnaseq/SRR6357076_2.fastq.gz"),
        ],
    ],
    "strandedness": [
        Strandedness.reverse,
        Strandedness.reverse,
        Strandedness.reverse,
    ],
    "fasta": LatchFile("s3://latch-public/welcome/nf_rnaseq/genome.fa.gz"),
    "gtf": LatchFile("s3://latch-public/welcome/nf_rnaseq/genes.gtf.gz"),
    "gene_bed": LatchFile("s3://latch-public/welcome/nf_rnaseq/genes.bed"),
    "output_dir": LatchDir("latch://nf_rnaseq_results/"),
}"""

# NOTE (kenny) ~ This is a poor test for the moment , but without mocking out
# the connection to Latch nucleus, we can rely on the boolean response as
# success.


def test_execute_previous_versions():

    with NamedTemporaryFile("w+") as tf:
        tf.write(simple_plan)
        tf.seek(0)

        assert launch(tf.name) == "wf.__init__.assemble_and_sort"
        assert launch(tf.name, "barrackobama") == "wf.__init__.assemble_and_sort"


def test_execute_rnaseq():

    with NamedTemporaryFile("w+") as tf:
        tf.write(rnaseq_plan)
        tf.seek(0)

        assert launch(tf.name) == "wf.__init__.nf_rnaseq_wf"


def test_execute_crispresso():

    with NamedTemporaryFile("w+") as tf:
        tf.write(crispresso_plan)
        tf.seek(0)

        assert launch(tf.name) == "wf.__init__.crispresso2_wf"
