import re
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from typing import Any, List, Optional, Tuple, Type, TypeVar

import click

import latch.types.metadata as metadata
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch_cli.snakemake.config.utils import get_preamble, type_repr
from latch_cli.snakemake.utils import reindent
from latch_cli.utils import identifier_from_str

template = """\
from dataclasses import dataclass
from enum import Enum
import os
import subprocess
import requests
import shutil
from pathlib import Path
import typing
import typing_extensions

from latch.resources.workflow import workflow
from latch.resources.tasks import nextflow_runtime_task, small_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch_cli.nextflow.workflow import get_flag
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

import latch_metadata


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
    return resp.json()["name"]


{preambles}


@nextflow_runtime_task
def nextflow_runtime(pvc_name: str, {param_signature}) -> None:
    workdir = Path("/nf-workdir")

    bin_dir = Path("/root/bin")
    shared_bin_dir = workdir / "bin"
    if bin_dir.exists():
        print("Copying module binaries...")
        shutil.copytree(bin_dir, shared_bin_dir)

    env = {{
        **os.environ,
        "NXF_HOME": "/root/.nextflow",
        "K8_STORAGE_CLAIM_NAME": pvc_name,
        "LATCH_SHARED_BIN_DIR": str(shared_bin_dir),
    }}
    try:
        subprocess.run(
            [
                "/root/.latch/bin/nextflow",
                "run",
                "{script_dir}",
                "-work-dir",
                str(workdir),
                "-profile",
                "{execution_profile}",
{params_to_flags}
            ],
            env=env,
            check=True,
        )
    except subprocess.CalledProcessError:
        log = Path("/root/.nextflow.log").read_text()
        print()
        print(log)
        raise


@workflow(metadata._nextflow_metadata)
def {workflow_func_name}({param_signature_with_defaults}) -> None:
    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, {param_args})

"""


def _get_flags_for_dataclass(name: str, val: Any) -> List[str]:
    assert is_dataclass(val)

    flags = []
    for f in fields(val):
        flags.extend(get_flag(f"{name}.{f.name}", getattr(val, f.name)))

    return flags


def get_flag(name: str, val: Any) -> List[str]:
    flag = f"--{name}"

    if isinstance(val, bool):
        return [flag] if val else []
    elif isinstance(val, LatchFile) or isinstance(val, LatchDir):
        if val.remote_path is not None:
            return [flag, val.remote_path]

        return [flag, str(val.path)]
    elif is_dataclass(val):
        return _get_flags_for_dataclass(name, val)
    elif isinstance(val, Enum):
        return [flag, getattr(val, "value")]
    else:
        return [flag, str(val)]


def generate_nextflow_workflow(
    pkg_root: Path,
    workflow_name: str,
    nf_script: Path,
    *,
    execution_profile: Optional[str] = None,
):
    assert metadata._nextflow_metadata is not None

    parameters = metadata._nextflow_metadata.parameters

    flags = []
    for param_name, param in parameters.items():
        flags.append(reindent(f"*get_flag({repr(param_name)}, {param_name})", 3))

    defaults: List[Tuple[str, str]] = []
    no_defaults: List[str] = []
    preambles: List[str] = []
    for param_name, param in parameters.items():
        sig = f"{param_name}: {type_repr(param.type)}"
        if param.default is not None:
            if isinstance(param.default, Enum):
                defaults.append((sig, param.default))
            else:
                defaults.append((sig, repr(param.default)))
        else:
            no_defaults.append(sig)

        preamble = get_preamble(param.type)
        if len(preamble) > 0:
            preambles.append(preamble)

    entrypoint = template.format(
        workflow_func_name=identifier_from_str(workflow_name),
        script_dir=nf_script.resolve().relative_to(pkg_root.resolve()),
        param_signature_with_defaults=", ".join(
            no_defaults + [f"{name}={val}" for name, val in defaults]
        ),
        param_signature=", ".join(no_defaults + [name for name, _ in defaults]),
        param_args=", ".join(
            f"{param_name}={param_name}" for param_name in parameters.keys()
        ),
        params_to_flags=",\n".join(flags),
        execution_profile=(
            execution_profile if execution_profile is not None else "standard"
        ),
        preambles="\n\n".join(preambles),
    )

    entrypoint_path = pkg_root / "wf" / "entrypoint.py"
    entrypoint_path.parent.mkdir(exist_ok=True)
    entrypoint_path.write_text(entrypoint)

    click.secho(
        f"Nextflow workflow written to {pkg_root / 'wf' / 'entrypoint.py'}",
        fg="green",
    )
