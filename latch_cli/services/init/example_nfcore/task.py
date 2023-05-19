import subprocess
from pathlib import Path

from latch import medium_task
from latch.functions.messages import message
from latch.types import LatchDir, LatchFile, LatchOutputDir


@medium_task
def run_nfcore_fetchngs(
    ids: LatchFile, output_directory: LatchOutputDir
) -> LatchDir:
    outdir = Path("/root/outputs").resolve()
    outdir.mkdir(exist_ok=True)

    nfcore_cmd = [
        "nextflow",
        "run",
        "nf-core/fetchngs",
        "--input",
        ids.local_path,
        "--outdir",
        str(outdir),
        "-profile",
        "docker",
    ]

    try:
        # When using shell=True, we pass the entire command as a single string as
        # opposed to a list since the shell will parse the string into a list
        # using its own rules.
        subprocess.run(" ".join(nfcore_cmd), shell=True, check=True)
    except subprocess.CalledProcessError as e:
        message(
            "error",
            {"title": "NFCore fetchngs Failed", "body": f"Error: {str(e)}"},
        )
        raise e

    return LatchDir(path=str(outdir), remote_path=output_directory.remote_path)
