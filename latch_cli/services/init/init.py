"""Service to initialize boilerplate."""

import re
import shutil
from datetime import datetime
from pathlib import Path
from typing import Callable, Optional

import click
from pkg_resources import get_distribution

from latch_cli.constants import latch_constants
from latch_cli.docker_utils import generate_dockerfile
from latch_cli.tui import select_tui
from latch_cli.types import LatchWorkflowConfig


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

    for f in source_path.glob("requirements*"):
        shutil.copy(f, pkg_root)

    for f in source_path.glob("env*"):
        shutil.copy(f, pkg_root)

    common_source = source_path.parent / "common"
    for f in common_source.iterdir():
        shutil.copy(f, pkg_root)

    version_f = pkg_root / "version"
    with open(version_f, "w") as f:
        f.write("0.0.0")


def _gen_assemble_and_sort(pkg_root: Path):
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "assemble_and_sort"

    _get_boilerplate(pkg_root, source_path)

    shutil.copytree(source_path / "bowtie2", pkg_root / "bowtie2")

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

    conda_env_dest = pkg_root / "environment.yml"
    conda_env_src = source_path / "environment.yml"
    shutil.copy(conda_env_src, conda_env_dest)


option_map = {
    "Empty workflow": _gen_template,
    "Subprocess Example": _gen_assemble_and_sort,
    "R Example": _gen_example_r,
    "Conda Example": _gen_example_conda,
}


template_flag_to_option = {
    "empty": "Empty workflow",
    "subprocess": "Subprocess Example",
    "r": "R Example",
    "conda": "Conda Example",
}


def init(
    pkg_name: str, template: Optional[str], expose_dockerfile: bool = True
) -> bool:
    """Creates boilerplate workflow files in the user's working directory.

    Args:
        pkg_name: A identifier for the workflow - will name the boilerplate
            directory as well as functions within the constructed package.
        expose_dockerfile: Whether to expose a Dockerfile in the workflow.
            If true, the Dockerfile will be created at init time and can be
            modified. Otherwise, the Dockerfile will be created at registration
            time and the user will not be able to modify it.

    Example:

        >>> init("test-workflow")
            # The resulting file structure will look like
            #   test-workflow
            #   ├── Dockerfile
            #   ├── version
            #   └── wf
            #       └── __init__.py

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

    config = LatchWorkflowConfig(
        latch_version=get_distribution("latch").version,
        base_image=latch_constants.base_image,
        date=datetime.now(),
    )

    (pkg_root / ".latch").mkdir(exist_ok=True)

    with open(pkg_root / latch_constants.pkg_config, "w") as f:
        f.write(config.to_json())

    template_func(pkg_root)

    if expose_dockerfile:
        generate_dockerfile(pkg_root, pkg_root / "Dockerfile")

    return True
