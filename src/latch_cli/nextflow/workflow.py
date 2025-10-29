import json
from dataclasses import Field, fields, is_dataclass
from enum import Enum
from pathlib import Path
from textwrap import dedent
from typing import Annotated, Callable, Optional, TypeVar, get_args, get_origin

import click
from flytekit.core.annotation import FlyteAnnotation

from latch.types import metadata
from latch.types.directory import LatchDir
from latch.types.file import LatchFile
from latch.types.samplesheet_item import SamplesheetItem
from latch_cli.constants import latch_constants
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

from latch.resources.workflow import nextflow_workflow
from latch.resources.tasks import nextflow_runtime_task, custom_task
from latch.types.file import LatchFile
from latch.types.directory import LatchDir, LatchOutputDir
from latch.ldata.path import LPath
from latch.executions import report_nextflow_used_storage
from latch_cli.nextflow.workflow import flags_from_args
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
        "http://nf-dispatcher-service.flyte.svc.cluster.local/provision-storage-ofs",
        headers=headers,
        json={{
            "storage_expiration_hours": {storage_expiration_hours},
            "version": 2,
        }},
    )
    resp.raise_for_status()
    print("Done.")

    return resp.json()["name"]


@nextflow_runtime_task(cpu={cpu}, memory={memory}, storage_gib={storage_gib})
def nextflow_runtime(pvc_name: str, args: latch_metadata.WorkflowArgsType) -> None:
    root_dir = Path("/root")
    shared_dir = Path("/nf-workdir")

    exec_name = _get_execution_name()
    if exec_name is None:
        print("Failed to get execution name.")
        exec_name = "unknown"

    latch_log_dir = urljoins("{log_dir}", exec_name)
    print(f"Log directory: {{latch_log_dir}}")

{output_shortcuts}

    to_ignore = {{
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
    }}

    for p in root_dir.iterdir():
        if p.name in to_ignore:
            continue

        src = root_dir / p.name
        target = shared_dir / p.name

        if p.is_dir():
            shutil.copytree(
                src,
                target,
                ignore_dangling_symlinks=True,
                dirs_exist_ok=True,
            )
        else:
            shutil.copy2(src, target)

    profile_list = {execution_profile}
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
        *flags_from_args(args, shared_dir),
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
            "NXF_ENABLE_FS_SYNC": "true",
        }}

        if {upload_command_logs}:
            env["LATCH_LOG_DIR"] = latch_log_dir

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
            remote = LPath(urljoins(latch_log_dir, "nextflow.log"))
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


@nextflow_workflow(metadata._nextflow_metadata)
def {workflow_func_name}(args: latch_metadata.WorkflowArgsType) -> None:
    \"\"\"
{docstring}
    \"\"\"

    pvc_name: str = initialize()
    nextflow_runtime(pvc_name=pvc_name, args=args)

"""


def _get_flags_for_dataclass(name: str, val: object) -> list[str]:
    assert is_dataclass(val)

    flags = []
    for f in fields(val):
        flags.extend(get_flag(f"{name}.{f.name}", getattr(val, f.name)))

    return flags


def get_flag(name: str, val: object) -> list[str]:
    flag = f"--{name}"

    if val is None:
        return []
    if isinstance(val, bool):
        return [flag] if val else []
    if isinstance(val, (LatchFile, LatchDir)):
        if val.remote_path is not None:
            return [flag, val.remote_path]

        return [flag, str(val.path)]
    if is_dataclass(val):
        return _get_flags_for_dataclass(name, val)
    if isinstance(val, Enum):
        return [flag, val.value]

    return [flag, str(val)]


def is_samplesheet_parameter(f: Field[object]) -> bool:
    origin = get_origin(f.type)
    if origin is not Annotated:
        return False

    args = get_args(f.type)
    assert len(args) > 1

    for ann in args:
        if not isinstance(ann, FlyteAnnotation):
            continue

        return ann.data.get("samplesheet", False)

    return False


T = TypeVar("T")


def get_flag_ext(
    f: Field[object],
    value: object,
    shared_dir: Path,
    samplesheet_constructor: Callable[[list[T], type[T]], Path],
) -> list[str]:
    if not is_samplesheet_parameter(f):
        return get_flag(f.name, value)

    # f.type = Annotated[List[Dataclass]]
    ann_args = get_args(f.type)
    assert len(ann_args) == 2

    list_typ = ann_args[0]
    list_args = get_args(list_typ)
    assert len(list_args) == 1

    item_type = list_args[0]
    item_origin = get_origin(item_type)
    if item_origin is SamplesheetItem:
        item_args = get_args(item_type)
        assert len(item_args) == 1
        item_type = item_args[0]
        value = [v.data for v in value]

    output_path = shared_dir / f"{f.name}_samplesheet.csv"

    res = samplesheet_constructor(value, item_type)
    try:
        output_path.write_text(res.read_text())
    finally:
        res.unlink()

    return [f"--{f.name}", str(output_path)]


def flags_from_args(
    args: object,
    shared_dir: Path,
    *,
    # todo(ayush): support samplesheet constructors per parameter
    samplesheet_constructor: Optional[Callable[[T, type[T]], Path]] = None,
) -> list[str]:
    assert is_dataclass(args)

    if samplesheet_constructor is None:
        samplesheet_constructor = metadata.default_samplesheet_constructor

    flags: list[str] = []
    for f in fields(args):
        flags.extend(
            get_flag_ext(f, getattr(args, f.name), shared_dir, samplesheet_constructor)
        )

    return flags


def generate_nextflow_config(pkg_root: Path):
    config_path = Path(pkg_root) / "latch.config"
    config_path.write_text(
        dedent("""\
        process {
            executor = 'k8s'
        }

        k8s {
            runAsUser = 0
        }

        aws {
            client {
                anonymous = true
            }
        }
        """)
    )

    click.secho(f"Nextflow Latch config written to {config_path}", fg="green")


def get_results_code_block(parameters: dict[str, metadata.NextflowParameter]) -> str:
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

    code_block += dedent("add_execution_results(results)\n")

    return code_block


def get_nextflow_major_version(pkg_root: Path) -> int:
    try:
        with (pkg_root / latch_constants.pkg_config).open("r") as f:
            config = LatchWorkflowConfig(**json.load(f))
    except FileNotFoundError:
        click.secho(
            dedent(f"""
            Could not find the latch config file at {pkg_root / latch_constants.pkg_config}

            Please check if you package root contains a Dockerfile that was NOT generated
            by the Latch CLI. If it does, please move it to a subdirectory and try again.
            """),
            fg="red",
        )
        raise click.exceptions.Exit(1)

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
        execution_profile=(
            execution_profile.split(",") if execution_profile is not None else []
        ),
        output_shortcuts=reindent(get_results_code_block(parameters), 1),
        cpu=resources.cpus,
        memory=resources.memory,
        heap_initial=int(java_heap_size / 4),
        heap_max=int(java_heap_size),
        storage_gib=resources.storage_gib,
        storage_expiration_hours=resources.storage_expiration_hours,
        log_dir=log_dir,
        upload_command_logs=metadata._nextflow_metadata.upload_command_logs,
    )

    dest.write_text(entrypoint)
    click.secho(f"Nextflow workflow written to {dest}", fg="green")
