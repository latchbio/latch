import subprocess
from pathlib import Path

from latch import small_task
from latch.functions.messages import message
from latch.types import LatchFile, LatchOutputDir


@small_task
def assembly_task(
    read1: LatchFile, read2: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:

    outdir = Path("/root/outputs").resolve()
    outdir.mkdir(exist_ok=True)

    bowtie2_cmd = [
        "docker",
        "run",
        "--user",
        "root",
        "--env",
        "BOWTIE2_INDEXES=/reference",
        "--mount",
        "type=bind,source=/root/reference,target=/reference",
        "--mount",
        f"type=bind,source={read1.local_path},target=/r1.fq",
        "--mount",
        f"type=bind,source={read2.local_path},target=/r2.fq",
        "--mount",
        f"type=bind,source={outdir},target=/outputs",
        "biocontainers/bowtie2:v2.4.1_cv1",
        "bowtie2",
        "--local",
        "--very-sensitive-local",
        "-x",
        "wuhan",
        "-1",
        "/r1.fq",
        "-2",
        "/r2.fq",
        "-S",
        "/outputs/covid_assembly.sam",
    ]

    try:
        # When using shell=True, we pass the entire command as a single string as
        # opposed to a list since the shell will parse the string into a list
        # using its own rules.
        subprocess.run(" ".join(bowtie2_cmd), shell=True, check=True)
    except subprocess.CalledProcessError as e:
        message(
            "error",
            {"title": "Bowtie2 Failed", "body": f"Error: {str(e)}"},
        )
        raise e

    # intended output path of the file in Latch console, constructed from
    # the user provided output directory
    output_location = f"{output_directory.remote_directory}/covid_assembly.sam"
    local_sam_file = outdir / "covid_assembly.sam"

    return LatchFile(str(local_sam_file), output_location)
