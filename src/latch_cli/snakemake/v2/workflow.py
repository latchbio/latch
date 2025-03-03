from pathlib import Path

import latch.types.metadata.snakemake_v2 as snakemake
from latch_cli.snakemake.config.utils import get_preamble, type_repr

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
from latch.resources.workflow import workflow
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile
from latch_cli.snakemake.v2.utils import get_config_val
from latch_cli.services.register.utils import import_module_by_path

import_module_by_path(Path({metadata_path}))

import latch.types.metadata.snakemake_v2 as smv2


{preambles}
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
def snakemake_runtime(pvc_name: str, {parameters}):
    print(f"Using shared filesystem: {{pvc_name}}")

    shared = Path("/snakemake-workdir")
    snakefile = shared / {snakefile_path}

    config = {{{config_builders}}}

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


@workflow(smv2._snakemake_v2_metadata)
def {workflow_name}({parameters}):
    \"\"\"
    Sample Description
    \"\"\"

    snakemake_runtime(pvc_name=initialize(), {assignments})
"""


def get_entrypoint_content(pkg_root: Path, metadata_path: Path, snakefile_path: Path) -> str:
    metadata = snakemake._snakemake_v2_metadata
    assert metadata is not None

    defined_names: set[str] = set()
    preambles: list[str] = []

    defaults: list[str] = []
    no_defaults: list[str] = []
    config_builders: list[str] = []
    assignments: list[str] = []

    for name, param in metadata.parameters.items():
        assert param.type is not None

        param_str = f"{name}: {type_repr(param.type)}"
        if param.default is None:
            no_defaults.append(param_str)
        else:
            param_str = f"{param_str} = {param.default!r}"
            defaults.append(param_str)

        config_builders.append(f"{name!r}: get_config_val({name})")
        assignments.append(f"{name}={name}")

        preambles.append(get_preamble(param.type, defined_names=defined_names))

    return _template.format(
        metadata_path=repr(str(metadata_path.relative_to(pkg_root))),
        preambles="".join(preambles),
        parameters=", ".join(no_defaults + defaults),
        snakefile_path=repr(str(snakefile_path.relative_to(pkg_root))),
        config_builders=", ".join(config_builders),
        workflow_name=metadata.name,
        assignments=", ".join(assignments),
    )
