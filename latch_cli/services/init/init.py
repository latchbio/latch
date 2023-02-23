"""Service to initialize boilerplate."""

import re
import shutil
import subprocess
from pathlib import Path
from typing import Callable, Optional

import click

from latch_cli.docker_utils import generate_dockerfile
from latch_cli.tui import select_tui
from latch_cli.workflow_config import BaseImageOptions, create_and_write_config


def _get_boilerplate(pkg_root: Path, source_path: Path):
    pkg_root = pkg_root.resolve()
    source_path = source_path.resolve()

    wf_root = pkg_root / "wf"
    wf_root.mkdir(exist_ok=True)

    for f in source_path.glob("*.py"):
        shutil.copy(f, wf_root)

    for f in source_path.glob("LICENSE*"):
        shutil.copy(f, pkg_root)

    for f in source_path.glob("README*"):
        shutil.copy(f, pkg_root)

    for f in source_path.glob("*requirements*"):
        shutil.copy(f, pkg_root)

    for f in source_path.glob("env*"):
        shutil.copy(f, pkg_root)

    if (source_path / ".env").exists():
        shutil.copy(source_path / ".env", pkg_root)

    common_source = source_path.parent / "common"
    for f in common_source.iterdir():
        shutil.copy(f, pkg_root)

    version_f = pkg_root / "version"
    with open(version_f, "w") as f:
        f.write("0.0.0")


def _get_example_reference(pkg_root: Path):
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    pkg_root = pkg_root.resolve()

    data_root = pkg_root / "reference"
    data_root.mkdir(exist_ok=True)

    ref_ids = [
        "wuhan.1.bt2",
        "wuhan.2.bt2",
        "wuhan.3.bt2",
        "wuhan.4.bt2",
        "wuhan.fasta",
        "wuhan.rev.1.bt2",
        "wuhan.rev.2.bt2",
    ]

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    print("Downloading workflow data ", flush=True, end="")
    for id in ref_ids:
        print(".", flush=True, end="")
        with open(data_root / id, "wb") as f:
            s3.download_fileobj("latch-public", f"sdk/{id}", f)
    print()


def _gen_assemble_and_sort(pkg_root: Path):
    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "assemble_and_sort"

    _get_boilerplate(pkg_root, source_path)

    _get_example_reference(pkg_root)

    print("Downloading bowtie2")
    bowtie2_base_name = "bowtie2-2.5.1-linux-x86_64"
    subprocess.run(
        [
            "curl",
            f"https://latch-public.s3.us-west-2.amazonaws.com/sdk/{bowtie2_base_name}.zip",
            "-o",
            str(pkg_root / f"{bowtie2_base_name}.zip"),
        ],
        check=True,
    )
    subprocess.run(
        ["unzip", str(pkg_root / f"{bowtie2_base_name}.zip"), "-d", str(pkg_root)],
        check=True,
    )

    bowtie_dir = pkg_root / "bowtie2"
    bowtie_dir.mkdir(exist_ok=True)

    subprocess.run(
        f"mv {str(pkg_root / bowtie2_base_name)}/*bowtie2* {str(pkg_root / 'bowtie2')}",
        check=True,
        shell=True,
    )
    subprocess.run(
        f"rm -r {str(pkg_root / bowtie2_base_name)} {str(pkg_root / bowtie2_base_name)}.zip {str(pkg_root / 'bowtie2')}/*-debug*",
        check=True,
        shell=True,
    )
    print()


def _gen_template(pkg_root: Path):
    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "template"

    _get_boilerplate(pkg_root, source_path)


def _gen_example_r(pkg_root: Path):
    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "example_r"

    _get_boilerplate(pkg_root, source_path)


def _gen_example_conda(pkg_root: Path):
    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "example_conda"

    _get_boilerplate(pkg_root, source_path)

    conda_env_dest = pkg_root / "environment.yaml"
    conda_env_src = source_path / "environment.yaml"
    shutil.copy(conda_env_src, conda_env_dest)


def _gen_example_docker(pkg_root: Path):
    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "assemble_and_sort"

    _get_boilerplate(pkg_root, source_path)
    _get_example_reference(pkg_root)

    source_docker_path = Path(__file__).parent / "example_docker"
    shutil.copy(source_docker_path / "assemble.py", pkg_root / "wf/assemble.py")

    (pkg_root / ".env").unlink()


option_map = {
    "Empty workflow": _gen_template,
    "Subprocess Example": _gen_assemble_and_sort,
    "R Example": _gen_example_r,
    "Conda Example": _gen_example_conda,
    "Docker Example": _gen_example_docker,
}


template_flag_to_option = {
    "empty": "Empty workflow",
    "docker": "Docker Example",
    "subprocess": "Subprocess Example",
    "r": "R Example",
    "conda": "Conda Example",
}


def init(
    pkg_name: str,
    template: Optional[str],
    expose_dockerfile: bool = True,
    base_image_type_str: str = "default",
) -> bool:
    """Creates boilerplate workflow files in the user's working directory.

    Args:
        pkg_name: A identifier for the workflow - will name the boilerplate
            directory as well as functions within the constructed package.
        template: A template to use for the workflow. If None, you will be
            prompted to choose a template.
                * "empty": An empty workflow wrapper
                * "subprocess": An example workflow that runs a subprocess
                * "r": A template workflow for executing an R script
                * "conda": A template workflow for executing code within a conda environment
        expose_dockerfile: Whether to expose a Dockerfile in the workflow.
            If true, the Dockerfile will be created at init time and can be
            modified. Otherwise, the Dockerfile will be created at registration
            time and the user will not be able to modify it. At any point,
            the user can switch modes by executing `latch dockerfile .` in
            the workflow directory.
        base_image_type_str: Base image to use for the workflow. Default value
            is "default". The following options are available:
                * "default": with no additional dependencies
                * "cuda": with Nvidia CUDA/cuDNN (cuda 11.4.2, cudnn 8) drivers
                * "opencl": with OpenCL (ubuntu 18.04) drivers
                * "docker": with the Docker daemon
    Example:

        >>> init("test-workflow", "empty", False)
            # The resulting file structure will look like
            #   test-workflow
            #   ├── version
            #   └── wf
            #       ├── __init__.py
            #       └── task.py

    """

    pkg_root = Path(pkg_name).resolve()
    pkg_name = pkg_root.name

    append_ctx_to_error: Callable[[str], str] = (
        lambda x: f"{x}. Current directory name: {pkg_root}"
        if pkg_root == Path.cwd()
        else f"{x}. Supplied name: {pkg_root}"
    )

    # Workflow name must not contain capitals or start or end in a hyphen or underscore. If it does, we should throw an error.

    if any(char.isupper() for char in pkg_name):
        raise ValueError(
            append_ctx_to_error(
                f"package name must not contain any upper-case characters: {pkg_name}"
            ),
        )

    if re.search("^[a-z]", pkg_name) is None:
        raise ValueError(
            append_ctx_to_error(
                f"package name must start with a lower-case letter: {pkg_name}"
            ),
        )

    if re.search("[a-z]$", pkg_name) is None:
        raise ValueError(
            append_ctx_to_error(
                f"package name must end with a lower-case letter: {pkg_name}"
            ),
        )

    for char in pkg_name:
        if not char.isalnum and char not in ["-", "_"]:
            raise ValueError(
                append_ctx_to_error(
                    f"package name must only contain alphanumeric characters, hyphens, and underscores: found `{char}`."
                ),
            )

    selected_option = (
        select_tui(
            title="Select Workflow Template",
            options=list(option_map.keys()),
        )
        if template is None
        else template_flag_to_option[template]
    )

    if base_image_type_str not in BaseImageOptions.__members__:
        raise ValueError(
            f"Invalid base image type: {base_image_type_str}. Must be one of {list(BaseImageOptions.__members__.keys())}"
        )


    if selected_option is None:
        return False

    template_func = option_map[selected_option]

    try:
        pkg_root.mkdir(parents=True)
    except FileExistsError:
        if not pkg_root.is_dir():
            raise ValueError(
                f"Cannot create directory `{pkg_name}`. A file with that name already exists."
            )

        if not click.confirm(
            f"Warning -- existing files in directory `{pkg_name}` may be overwritten by boilerplate. Continue?"
        ):
            return False

    template_func(pkg_root)

    base_image_type = BaseImageOptions.__members__[base_image_type_str]

    if selected_option == "Docker Example":
        base_image_type = BaseImageOptions.docker

    create_and_write_config(pkg_root, base_image_type)

    if expose_dockerfile:
        generate_dockerfile(pkg_root, pkg_root / "Dockerfile")

    return True
