import json
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Any, Dict, List, Optional, Tuple

import click

import latch.types.metadata as metadata
from latch.types.directory import LatchDir, LatchOutputDir
from latch.types.file import LatchFile
from latch.types.metadata import NextflowParameter
from latch_cli.constants import latch_constants
from latch_cli.snakemake.config.utils import get_preamble, type_repr
from latch_cli.snakemake.utils import reindent
from latch_cli.utils import identifier_from_str, urljoins
from latch_cli.workflow_config import LatchWorkflowConfig

template = """\
import sys
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
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch.executions import report_nextflow_used_storage
from latch_cli.nextflow.workflow import get_flag
from latch_cli.nextflow.utils import _get_execution_name
from latch_cli.utils import urljoins
from latch.types import metadata
from flytekit.core.annotation import FlyteAnnotation

from latch_cli.services.register.utils import import_module_by_path

meta = Path("{metadata_root}") / "__init__.py"
import_module_by_path(meta)
import latch_metadata

@custom_task(cpu=0.25, memory=0.5, storage_gib=1)
def initialize() -> str:
    token = os.environ.get("FLYTE_INTERNAL_EXECUTION_ID")
    if token is None:
        raise RuntimeError("failed to get execution token")

    headers = {{"Authorization": f"Latch-Execution-Token {{token}}"}}

    print("Provisioning shared storage volume... ", end="")
    resp = requests.post(
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage",
        headers=headers,
        json={{
            "storage_expiration_hours": {storage_expiration_hours},
            "version": {nextflow_version},
        }},
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]


{preambles}

{samplesheet_funs}

@nextflow_runtime_task(cpu={cpu}, memory={memory}, storage_gib={storage_gib})
def nextflow_runtime(pvc_name: str, {param_signature}) -> None:
    shared_dir = Path("/nf-workdir")

{output_shortcuts}
{samplesheet_constructors}

    ignore_list = [
        "latch",
        ".latch",
        ".git",
        "nextflow",
        ".nextflow",
        "work",
        "results",
        "miniconda",
        "anaconda3",
        "mambaforge",
    ]

    shutil.copytree(
        Path("/root"),
        shared_dir,
        ignore=lambda src, names: ignore_list,
        ignore_dangling_symlinks=True,
        dirs_exist_ok=True,
    )

    profile_list = {execution_profile}
    if {configurable_profiles}:
        profile_list.extend([p.value for p in execution_profiles])

    if len(profile_list) == 0:
        profile_list.append("standard")

    profiles = ','.join(profile_list)

    cmd = [
        "/root/nextflow",
        "run",
        str(shared_dir / "{nf_script}"),
        "-work-dir",
        str(shared_dir),
        "-profile",
        profiles,
        "-c",
        "latch.config",
        "-resume",
{params_to_flags}
    ]

    print("Launching Nextflow Runtime")
    print(' '.join(cmd))
    print(flush=True)

    failed = False
    try:
        env = {{
            **os.environ,
            "NXF_ANSI_LOG": "false",
            "NXF_HOME": "/root/.nextflow",
            "NXF_OPTS": "-Xms{heap_initial}M -Xmx{heap_max}M -XX:ActiveProcessorCount={cpu}",
            "NXF_DISABLE_CHECK_LATEST": "true",
            "NXF_ENABLE_VIRTUAL_THREADS": "false",
        }}
        subprocess.run(
            cmd,
            env=env,
            check=True,
            cwd=str(shared_dir),
        )
    except subprocess.CalledProcessError:
        failed = True
    finally:
        print()

        nextflow_log = shared_dir / ".nextflow.log"
        if nextflow_log.exists():
            name = _get_execution_name()
            if name is None:
                print("Skipping logs upload, failed to get execution name")
            else:
                remote = LPath(urljoins("{log_dir}", name, "nextflow.log"))
                print(f"Uploading .nextflow.log to {{remote.path}}")
                remote.upload_from(nextflow_log)

        print("Computing size of workdir... ", end="")
        try:
            result = subprocess.run(
                ['du', '-sb', str(shared_dir)],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                timeout=5 * 60
            )

            size = int(result.stdout.split()[0])
            report_nextflow_used_storage(size)
            print(f"Done. Workdir size: {{size / 1024 / 1024 / 1024: .2f}} GiB")
        except subprocess.TimeoutExpired:
            print("Failed to compute storage size: Operation timed out after 5 minutes.")
        except subprocess.CalledProcessError as e:
            print(f"Failed to compute storage size: {{e.stderr}}")
        except Exception as e:
            print(f"Failed to compute storage size: {{e}}")

    if failed:
        sys.exit(1)


@workflow(metadata._nextflow_metadata)
def {workflow_func_name}({param_signature_with_defaults}) -> None:
    \"\"\"
{docstring}
    \"\"\"

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

    if val is None:
        return []
    elif isinstance(val, bool):
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


def generate_nextflow_config(pkg_root: Path):
    config_path = Path(pkg_root) / "latch.config"
    config_path.write_text(dedent("""\
        process {
            executor = 'k8s'
        }

        aws {
            client {
                anonymous = true
            }
        }
        """))

    click.secho(f"Nextflow Latch config written to {config_path}", fg="green")


def get_results_code_block(parameters: Dict[str, NextflowParameter]) -> str:
    output_shortcuts = [
        (var_name, sub_path)
        for var_name, param in parameters.items()
        if param.results_paths is not None
        for sub_path in param.results_paths
    ]

    if len(output_shortcuts) == 0:
        return ""

    code_block = dedent("""
    from latch.executions import add_execution_results

    results = []
    """)

    for var_name, sub_path in output_shortcuts:
        code_block += dedent(
            f"results.append(os.path.join({var_name}.remote_path,"
            f" '{str(sub_path).lstrip('/')}'))\n"
        )

    code_block += dedent(f"add_execution_results(results)\n")

    return code_block


def get_nextflow_major_version(pkg_root: Path) -> int:
    with (pkg_root / latch_constants.pkg_config).open("r") as f:
        config = LatchWorkflowConfig(**json.load(f))

    if "latch-base-nextflow" not in config.base_image:
        return 1

    version = config.base_image.split(":")[-1]
    return int(version[1])


def generate_nextflow_workflow(
    pkg_root: Path,
    metadata_root: Path,
    nf_script: Path,
    dest: Path,
    *,
    execution_profile: Optional[str] = None,
):
    generate_nextflow_config(pkg_root)

    assert metadata._nextflow_metadata is not None

    wf_name = metadata._nextflow_metadata.name
    assert wf_name is not None

    parameters = metadata._nextflow_metadata.parameters
    resources = metadata._nextflow_metadata.runtime_resources

    java_heap_size = resources.memory * 1024 * 0.75

    flags = []
    defaults: List[Tuple[str, str]] = []
    no_defaults: List[str] = []
    preambles: set[str] = set()
    samplesheet_funs: List[str] = []
    samplesheet_constructors: List[str] = []

    nextflow_task_args = ", ".join(
        f"{param_name}={param_name}" for param_name in parameters.keys()
    )

    profile_options = metadata._nextflow_metadata.execution_profiles
    if len(profile_options) > 0:
        profiles_var = "execution_profiles"

        execution_profile_enum = "class ExecutionProfile(Enum):\n"
        for profile in profile_options:
            execution_profile_enum += f"    {profile} = {repr(profile)}\n"

        no_defaults.append(f"{profiles_var}: list[ExecutionProfile]")
        nextflow_task_args += f", {profiles_var}={profiles_var}"
        preambles.add(execution_profile_enum)

    for param_name, param in parameters.items():
        sig = f"{param_name}: {type_repr(param.type)}"
        if param.default is not None:
            if isinstance(param.default, Enum):
                defaults.append((sig, param.default))
            elif param.type in {LatchDir, LatchFile}:
                defaults.append((
                    sig,
                    f"{param.type.__name__}('{param.default._raw_remote_path}')",
                ))
            elif param.type is LatchOutputDir:
                defaults.append((
                    sig,
                    f"LatchOutputDir('{param.default._raw_remote_path}')",
                ))
            else:
                defaults.append((sig, repr(param.default)))
        else:
            no_defaults.append(sig)

        if param.samplesheet:
            samplesheet_funs.append(
                reindent(
                    f"""
                    {param_name}_construct_samplesheet = metadata._nextflow_metadata.parameters[{repr(param_name)}].samplesheet_constructor
                    """,
                    0,
                ),
            )

            samplesheet_constructors.append(
                reindent(
                    f"{param_name}_samplesheet ="
                    f" {param_name}_construct_samplesheet({param_name})",
                    1,
                ),
            )

            flags.append(
                reindent(
                    f"*get_flag({repr(param_name)}, {param_name}_samplesheet)",
                    2,
                )
            )
        else:
            flags.append(reindent(f"*get_flag({repr(param_name)}, {param_name})", 4))

        preamble = get_preamble(param.type)
        if len(preamble) > 0 and preamble not in preambles:
            preambles.add(preamble)

    if metadata._nextflow_metadata.log_dir is None:
        log_dir = "latch:///nextflow_logs"
    else:
        log_dir = metadata._nextflow_metadata.log_dir._raw_remote_path
    log_dir = urljoins(log_dir, wf_name)

    desc = f"Sample Description"
    about_page_path = metadata._nextflow_metadata.about_page_path
    if about_page_path is not None:
        if about_page_path is not None:
            if not (about_page_path.exists() and about_page_path.is_file()):
                click.secho(
                    dedent(f"""
                    The about page path provided in the metadata is not a valid file:
                    {about_page_path}
                    """),
                    fg="red",
                )
                raise click.exceptions.Exit(1)
        desc = about_page_path.read_text()

    display_name = wf_name
    if metadata._nextflow_metadata.display_name is not None:
        display_name = metadata._nextflow_metadata.display_name

    docstring = f"{display_name}\n\n{desc}"

    entrypoint = template.format(
        workflow_func_name=identifier_from_str(wf_name),
        docstring=reindent(docstring, 1),
        nf_script=nf_script.resolve().relative_to(pkg_root.resolve()),
        metadata_root=str(metadata_root.resolve().relative_to(pkg_root.resolve())),
        param_signature_with_defaults=", ".join(
            no_defaults + [f"{name} = {val}" for name, val in defaults]
        ),
        param_signature=", ".join(no_defaults + [name for name, _ in defaults]),
        param_args=nextflow_task_args,
        params_to_flags=",\n".join(flags),
        execution_profile=(
            execution_profile.split(",") if execution_profile is not None else []
        ),
        output_shortcuts=reindent(get_results_code_block(parameters), 1),
        configurable_profiles=len(profile_options) > 0,
        preambles="\n\n".join(list(preambles)),
        samplesheet_funs="\n".join(samplesheet_funs),
        samplesheet_constructors="\n".join(samplesheet_constructors),
        cpu=resources.cpus,
        memory=resources.memory,
        heap_initial=int(java_heap_size / 4),
        heap_max=int(java_heap_size),
        storage_gib=resources.storage_gib,
        storage_expiration_hours=resources.storage_expiration_hours,
        log_dir=log_dir,
        nextflow_version=get_nextflow_major_version(pkg_root),
    )

    dest.write_text(entrypoint)
    click.secho(f"Nextflow workflow written to {dest}", fg="green")
