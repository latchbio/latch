from latch_cli.services.launch.launch_v2 import launch

launch(
    wf_name="nf_nf_core_rnaseq",
    version="3.14.0-at0-0c9139-wip-2a48ce",
    params={
        "fastq_dir": "latch:///test_data/fastq",
        "fastq_pattern": "*.fastq.gz",
        "fastq_dir_2": "latch:///test_data/fastq_2",
        "fastq_pattern_2": "*.fastq.gz",
    },
)
