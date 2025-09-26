from pathlib import Path

import latch.types.metadata.snakemake_v2 as snakemake

_template = """\
import json
import os
import shutil
import subprocess
import sys
import typing
import typing_extensions
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

import requests

from latch.resources.tasks import custom_task, snakemake_runtime_task
from latch.resources.workflow import snakemake_workflow
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile
from latch_cli.snakemake.v2.utils import get_config_val
from latch_cli.services.register.utils import import_module_by_path

latch_metadata = import_module_by_path(Path({metadata_path}))

import latch.types.metadata.snakemake_v2 as smv2


@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {{"Authorization": f"Latch-Execution-Token {{token}}"}}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage-ofs",
        headers=headers,
        json={{
            "storage_expiration_hours": 0,
            "version": 2,
            "snakemake": True,
        }},
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]

@snakemake_runtime_task(cpu=1, memory=2, storage_gib=50)
def snakemake_runtime(pvc_name: str, args: latch_metadata.WorkflowArgsType):
    print(f"Using shared filesystem: {{pvc_name}}")

    shared = Path("/snakemake-workdir")
    snakefile = shared / {snakefile_path}

    config = get_config_val(args)

    config_path = (shared / "__latch.config.json").resolve()
    config_path.write_text(json.dumps(config, indent=2))

    ignore_list = [
        "latch",
        ".latch",
        ".git",
        "nextflow",
        ".nextflow",
        ".snakemake",
        "results",
        "miniconda",
        "anaconda3",
        "mambaforge",
    ]

    shutil.copytree(
        Path("/root"),
        shared,
        ignore=lambda src, names: ignore_list,
        ignore_dangling_symlinks=True,
        dirs_exist_ok=True,
    )

    cmd = [
        "snakemake",
        "--snakefile",
        str(snakefile),
        "--configfile",
        str(config_path),
        "--executor",
        "latch",
        "--default-storage-provider",
        "latch",
        "--jobs",
        "1000",
    ]

    print("Launching Snakemake Runtime")
    print(" ".join(cmd), flush=True)

    failed = False
    try:
        subprocess.run(cmd, cwd=shared, check=True)
    except subprocess.CalledProcessError:
        failed = True
    finally:
        if not failed:
            return

        sys.exit(1)


@snakemake_workflow(smv2._snakemake_v2_metadata)
def {workflow_name}(args: latch_metadata.WorkflowArgsType):
    \"\"\"
    Sample Description
    \"\"\"

    snakemake_runtime(pvc_name=initialize(), args=args)
"""


def get_entrypoint_content(pkg_root: Path, metadata_path: Path, snakefile_path: Path) -> str:
    metadata = snakemake._snakemake_v2_metadata
    assert metadata is not None

    return _template.format(
        metadata_path=repr(str(metadata_path.relative_to(pkg_root))),
        snakefile_path=repr(str(snakefile_path.relative_to(pkg_root))),
        workflow_name=metadata.name,
    )
