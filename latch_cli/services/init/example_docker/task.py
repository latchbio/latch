import subprocess
from pathlib import Path
from typing import List

from latch.functions.messages import message
from latch.resources.tasks import small_task
from latch.types.directory import LatchOutputDir
from latch.types.file import LatchFile


def run(cmd: List[str]):
    try:
        return subprocess.run(cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode("utf-8")
        if stderr != "":
            message(
                "error",
                {"title": "Blastp Failed", "body": f"Stderr: {stderr}"},
            )
        print(stderr)
        raise e


@small_task
def blastp_task(
    query_fasta: LatchFile, database: LatchFile, output_directory: LatchOutputDir
) -> LatchFile:
    outdir = Path("/root/outputs").resolve()
    outdir.mkdir(exist_ok=True)

    database_path = Path(database.local_path)
    query_file_path = Path(query_fasta.local_path)

    db_prep = [
        "docker",
        "run",
        "--user",
        "root",  # run as root to avoid permission issues
        "-v",
        f"{database_path.parent}:/data/",  # mount the database file directory into the container
        "biocontainers/blast:2.2.31",
        "makeblastdb",
        "-in",
        f"/data/{database_path.name}",
        "-dbtype",
        "prot",
    ]

    db_prep_run = run(db_prep)
    print(db_prep_run.stdout.decode("utf-8"))

    # run blast
    blast_cmd = [
        "docker",
        "run",
        "--user",  # run as root to avoid permission issues
        "root",
        "-v",
        f"{query_file_path.parent}:/data/",  # mount the query file directory into the container
        "-v",
        f"{database_path.parent}:/db/",  # mount the database file directory into the container
        "-v",
        f"{outdir}:/output/",  # mount the output directory into the container
        "biocontainers/blast:2.2.31",
        "blastp",
        "-query",
        f"/data/{Path(query_fasta.local_path).name}",
        "-db",
        f"/db/{database_path.name}",
        "-out",
        f"/output/results.txt",
    ]

    blast_run = run(blast_cmd)
    print(blast_run.stdout.decode("utf-8"))

    # intended output path of the file in Latch console, constructed from
    # the user provided output directory
    output_location = (
        f"{output_directory.remote_directory}results.txt"
        if output_directory.remote_directory.endswith("/")
        else f"{output_directory.remote_directory}/results.txt"
    )
    local_sam_file = outdir / "results.txt"

    return LatchFile(str(local_sam_file), output_location)
