import re
from pathlib import Path
from typing import List, Optional, Tuple

import click

import latch.types.metadata as metadata
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.snakemake.config.utils import type_repr
from latch_cli.snakemake.utils import reindent

ENTRYPOINT_TEMPLATE = """import os
import subprocess
import requests
from pathlib import Path

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, small_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir


@small_task
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    print("Provisioning shared storage volume...")
    headers = {{"Authorization": f"Latch-Execution-Token {{token}}"}}
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
    )
    resp.raise_for_status()
    print("Done.")
    return resp.json()["name"]


@nextflow_runtime_task
def nextflow_runtime(pvc_name: str, {param_signature}) -> None:
{file_downloads}
    env = {{
        **os.environ,
        "NXF_HOME": "/root/.nextflow",
        "K8_STORAGE_CLAIM_NAME": pvc_name,
    }}
    subprocess.run(
        [
            "/root/.latch/bin/nextflow",
            "run",
            "{script_dir}",
            "-work-dir",
            "/nf-workdir",
            "-profile",
            "{execution_profile}",
{params_to_flags}
        ],
        env=env,
        check=True,
    )


@workflow
def {workflow_func_name}({param_signature_with_defaults}) -> None:
    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, {param_args})

"""


def generate_nextflow_workflow(
    pkg_root: Path,
    workflow_name: str,
    nf_script: Path,
    *,
    execution_profile: Optional[str] = None,
):
    assert metadata._nextflow_metadata is not None

    parameters = metadata._nextflow_metadata.parameters

    flags_str = ""
    download_str = ""
    for param_name, param in parameters.items():
        flags_str += reindent(
            f"""
            "--{param_name}",
            """,
            3,
        )
        if param.type in {LatchFile, LatchDir}:
            download_str += reindent(
                f"""
                {param_name}_path = Path({param_name}).resolve()
                """,
                1,
            )
            flags_str += reindent(
                f"""
                {param_name}_path,
                """,
                3,
            )
        else:
            flags_str += reindent(
                f"""
                str({param_name}),
                """,
                3,
            )
    flags_str = flags_str[:-1]

    defaults: List[Tuple[str, str]] = []
    no_defaults: List[str] = []
    for param_name, param in parameters.items():
        sig = f"{param_name}: {type_repr(param.type)}"
        if param.default is not None:
            defaults.append((sig, repr(param.default)))
        else:
            no_defaults.append(sig)

    entrypoint = ENTRYPOINT_TEMPLATE.format(
        # source: https://stackoverflow.com/questions/3303312/how-do-i-convert-a-string-to-a-valid-variable-name-in-python
        workflow_func_name=re.sub("\W|^(?=\d)", "_", workflow_name),
        script_dir=nf_script.resolve().relative_to(pkg_root.resolve()),
        param_signature_with_defaults=", ".join(
            no_defaults + [f"{p[0]} = {p[1]}" for p in defaults]
        ),
        param_signature=", ".join(no_defaults + [p[0] for p in defaults]),
        param_args=", ".join(
            f"{param_name}={param_name}" for param_name in parameters.keys()
        ),
        params_to_flags=flags_str,
        file_downloads=download_str,
        execution_profile=(
            execution_profile if execution_profile is not None else "standard"
        ),
    )

    with open(pkg_root / "wf" / "entrypoint.py", "w") as f:
        f.write(entrypoint)

    click.secho(
        f"Nextflow workflow written to  {pkg_root / 'wf' / 'entrypoint.py'}",
        fg="green",
    )
