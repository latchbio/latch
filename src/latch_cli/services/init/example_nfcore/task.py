import subprocess
from pathlib import Path

from latch.functions.messages import message
from latch.resources.tasks import medium_task
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile


@medium_task
def run_nfcore_fetchngs(ids: LatchFile, output_directory: LatchOutputDir) -> LatchDir:
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
        subprocess.run(nfcore_cmd, check=True, capture_output=True)
    except subprocess.CalledProcessError as e:
        stderr = e.stderr.decode("utf-8")
        if stderr:
            message(
                "error",
                {"title": "Blastp Failed", "body": f"Stderr: {stderr}"},
            )
        print(stderr)
        raise e

    return LatchDir(path=str(outdir), remote_path=output_directory.remote_path)
