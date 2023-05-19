import subprocess
from pathlib import Path
from typing import List, Union

from latch import medium_task
from latch.functions.messages import message
from latch.types import LatchDir, LatchFile, LatchOutputDir


@medium_task
def run_nfcore_fetchngs(
    ids: LatchFile, output_directory: LatchOutputDir
) -> List[Union[LatchFile, LatchDir]]:
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

    res = []

    for output_file_or_dir in outdir.glob("*"):
        if output_file_or_dir.is_dir():
            res.append(
                LatchDir(
                    path=str(output_file_or_dir),
                    remote_path=f"{output_directory.remote_path}/{output_file_or_dir.name}",
                )
            )
        else:
            res.append(

                LatchFile(
                    path=str(output_file_or_dir),
                    remote_path=f"{output_directory.remote_path}/{output_file_or_dir.name}",
                )
            )

    return res
