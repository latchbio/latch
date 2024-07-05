import json
import os
from dataclasses import dataclass
from enum import Enum, auto
from io import TextIOWrapper
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import click
import yaml

from latch_cli.constants import latch_constants
from latch_cli.utils import WorkflowType
from latch_cli.workflow_config import (
    BaseImageOptions,
    LatchWorkflowConfig,
    create_and_write_config,
)


class DockerCmdBlockOrder(str, Enum):
    """Put a command block before or after the primary COPY command."""

    precopy = auto()
    postcopy = auto()


@dataclass(frozen=True)
class DockerCmdBlock:
    comment: str
    commands: List[str]
    order: DockerCmdBlockOrder

    def write_block(self, f: TextIOWrapper):
        f.write(f"# {self.comment}\n")
        f.write("\n".join(self.commands) + "\n\n")


def get_prologue(
    config: LatchWorkflowConfig, wf_type: WorkflowType = WorkflowType.latchbiosdk
) -> List[str]:
    if wf_type == WorkflowType.snakemake:
        library_name = '"latch[snakemake]"'
    else:
        library_name = "latch"

    directives = [
        "# DO NOT CHANGE",
        f"from {config.base_image}",
        "",
        "workdir /tmp/docker-build/work/",
        "",
        dedent(r"""
        shell [ \
            "/usr/bin/env", "bash", \
            "-o", "errexit", \
            "-o", "pipefail", \
            "-o", "nounset", \
            "-o", "verbose", \
            "-o", "errtrace", \
            "-O", "inherit_errexit", \
            "-O", "shift_verbose", \
            "-c" \
        ]
        """).strip(),
        "env TZ='Etc/UTC'",
        "env LANG='en_US.UTF-8'",
        "",
        "arg DEBIAN_FRONTEND=noninteractive",
        "",
        "# Latch SDK",
        "# DO NOT REMOVE",
        f"run pip install {library_name}=={config.latch_version}",
        "run mkdir /opt/latch",
    ]

    return directives


def get_epilogue(wf_type: WorkflowType = WorkflowType.latchbiosdk) -> List[str]:
    cmds: list[str] = []

    cmds += [
        "",
        "# Latch workflow registration metadata",
        "# DO NOT CHANGE",
        "arg tag",
        "# DO NOT CHANGE",
        "env FLYTE_INTERNAL_IMAGE $tag",
        "",
        "workdir /root",
    ]

    return cmds


def infer_commands(pkg_root: Path) -> List[DockerCmdBlock]:
    commands: List[DockerCmdBlock] = []

    if (pkg_root / "system-requirements.txt").exists():
        click.echo(
            " ".join([
                click.style(f"system-requirements.txt:", bold=True),
                "System dependencies installation phase",
            ])
        )

        commands.append(
            DockerCmdBlock(
                comment="Install system dependencies",
                commands=[
                    "copy system-requirements.txt /opt/latch/system-requirements.txt",
                    dedent(r"""
                            run apt-get update --yes && \
                                xargs apt-get install --yes \
                                    < /opt/latch/system-requirements.txt
                            """).strip(),
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    if (pkg_root / "environment.R").exists():
        click.echo(
            " ".join([
                click.style(f"environment.R:", bold=True),
                "R dependencies installation phase",
            ])
        )

        # todo(maximsmol): allow specifying R version
        # todo(maximsmol): somehow promote using pak
        commands += [
            DockerCmdBlock(
                comment="Install rig the R installation manager",
                commands=[
                    dedent(r"""
                            run \
                                curl \
                                    --location \
                                    --fail \
                                    --remote-name \
                                    https://github.com/r-lib/rig/releases/download/latest/rig-linux-latest.tar.gz && \
                                tar \
                                    --extract \
                                    --gunzip \
                                    --file rig-linux-latest.tar.gz \
                                    --directory /usr/local/ && \
                                rm rig-linux-latest.tar.gz
                            """).strip(),
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Install R",
                commands=[
                    "run rig add release # Change to any R version",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Install R dependencies",
                commands=[
                    "copy environment.R /opt/latch/environment.R",
                    "run Rscript /opt/latch/environment.R",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
        ]

    conda_env_p = pkg_root / "environment.yml"
    if not conda_env_p.exists():
        conda_env_p = conda_env_p.with_suffix(".yaml")

    if conda_env_p.exists():
        click.echo(
            " ".join([
                click.style(f"{conda_env_p.name}:", bold=True),
                "Conda dependencies installation phase",
            ])
        )

        with conda_env_p.open("rb") as f:
            conda_env = yaml.safe_load(f)

        if "name" in conda_env:
            env_name = conda_env["name"]
        else:
            env_name = "workflow"

        # todo(maximsmol): install `curl` and other build deps ahead of time once (or in base image)
        commands += [
            DockerCmdBlock(
                comment="Install Mambaforge",
                commands=[
                    dedent(r"""
                            run apt-get update --yes && \
                                apt-get install --yes curl && \
                                curl \
                                    --location \
                                    --fail \
                                    --remote-name \
                                    https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh && \
                                `# Docs for -b and -p flags: https://docs.anaconda.com/anaconda/install/silent-mode/#linux-macos` \
                                bash Mambaforge-Linux-x86_64.sh -b -p /opt/conda -u && \
                                rm Mambaforge-Linux-x86_64.sh
                            """).strip(),
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Set conda PATH",
                commands=[
                    "env PATH=/opt/conda/bin:$PATH",
                    "RUN conda config --set auto_activate_base false",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Build conda environment",
                commands=[
                    f"copy {conda_env_p.name} /opt/latch/environment.yaml",
                    dedent(rf"""
                            run mamba env create \
                                --file /opt/latch/environment.yaml \
                                --name {env_name}
                            """).strip(),
                    f"env PATH=/opt/conda/envs/{env_name}/bin:$PATH",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
        ]

    has_setup_py = (pkg_root / "setup.py").exists()

    has_buildable_pyproject = False
    try:
        with (pkg_root / "pyproject.toml").open("r") as f:
            for line in f:
                if not line.startswith("[build-system]"):
                    continue

                has_buildable_pyproject = True
                break
    except FileNotFoundError:
        ...

    # from https://peps.python.org/pep-0518/ and https://peps.python.org/pep-0621/
    if has_setup_py or has_buildable_pyproject:
        cause = "setup.py" if has_setup_py else "pyproject.toml"
        click.echo(
            " ".join([
                click.style(f"{cause}:", bold=True),
                "Python package installation phase",
            ])
        )

        print()
        commands.append(
            DockerCmdBlock(
                comment="Install python package",
                commands=["run pip install /root/"],
                order=DockerCmdBlockOrder.postcopy,
            )
        )

    if (pkg_root / "requirements.txt").exists():
        click.echo(
            " ".join([
                click.style("requirements.txt:", bold=True),
                "Python pip dependencies installation phase",
            ])
        )
        commands.append(
            DockerCmdBlock(
                comment="Install pip dependencies from `requirements.txt`",
                commands=[
                    "copy requirements.txt /opt/latch/requirements.txt",
                    "run pip install --requirement /opt/latch/requirements.txt",
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    if (pkg_root / ".env").exists():
        click.echo(
            " ".join([click.style(".env:", bold=True), "Environment variable setup"])
        )
        envs: list[str] = []
        for line in (pkg_root / ".env").read_text().splitlines():
            line = line.strip()

            if line == "":
                continue
            if line.startswith("#"):
                continue

            envs.append(f"env {line}")

        commands.append(
            DockerCmdBlock(
                comment="Set environment variables",
                commands=envs,
                order=DockerCmdBlockOrder.precopy,
            )
        )

    return commands


def copy_file_commands(wf_type: WorkflowType) -> List[str]:
    cmd = [
        "",
        "# Copy workflow data (use .dockerignore to skip files)",
        "",
        "copy . /root/",
    ]

    if wf_type == WorkflowType.snakemake:
        cmd += [
            "",
            "# Latch snakemake workflow entrypoint",
            "# DO NOT CHANGE",
            "",
            "copy .latch/snakemake_jit_entrypoint.py /root/snakemake_jit_entrypoint.py",
        ]

    return cmd


def generate_dockerignore(pkg_root: Path, *, wf_type: WorkflowType) -> None:
    dest = Path(pkg_root) / ".dockerignore"
    if dest.exists():
        if os.path.isdir(dest):
            click.secho(
                f".dockerignore already exists at `{dest}` and is a directory.",
                fg="red",
            )
            raise click.exceptions.Exit(1)

        if not click.confirm(f".dockerignore already exists at `{dest}`. Overwrite?"):
            return

    with Path(".dockerignore").open("w") as f:
        files = [
            ".git",
            ".github",
        ]

        if wf_type == WorkflowType.nextflow:
            files.extend([
                ".nextflow*",
                ".nextflow.log*",
                "work/",
                "results/",
            ])

        dest.write_text(dedent("\n".join(files) + "\n"))

    click.secho(f"Successfully generated .dockerignore `{dest}`", fg="green")


def generate_dockerfile(
    pkg_root: Path,
    *,
    dest: Optional[Path] = None,
    wf_type: WorkflowType = WorkflowType.latchbiosdk,
    overwrite: bool = False,
) -> None:
    """Generate a best effort Dockerfile from files in the workflow directory.

    Args:
        pkg_root: A path to a workflow directory.
        dest: The path to write the generated Dockerfile. If None, write Dockerfile to the pkg_root.
        wf_type: The type of workflow (eg. snakemake) the Dockerfile is for

    Example:

        >>> generate_dockerfile(Path("test-workflow"), Path("test-workflow/Dockerfile"))
            # The resulting file structure will look like
            #   test-workflow
            #   ├── Dockerfile
            #   └── ...
    """
    if dest is None:
        dest = pkg_root / "Dockerfile"
    if (
        dest.exists()
        and not overwrite
        and not (click.confirm(f"Dockerfile already exists at `{dest}`. Overwrite?"))
    ):
        return

    click.secho("Generating Dockerfile", bold=True)
    try:
        with (pkg_root / latch_constants.pkg_config).open("r") as f:
            config: LatchWorkflowConfig = LatchWorkflowConfig(**json.load(f))
    except FileNotFoundError:
        click.secho("Creating a default configuration file")

        base_image_type = BaseImageOptions.default
        if wf_type == WorkflowType.nextflow:
            base_image_type = BaseImageOptions.nextflow

        create_and_write_config(pkg_root, base_image_type)
        with (pkg_root / latch_constants.pkg_config).open("r") as f:
            config = LatchWorkflowConfig(**json.load(f))

    click.echo(
        " ".join([
            click.style("Base image:", fg="bright_blue"),
            config.base_image,
        ])
    )
    click.echo(
        " ".join([
            click.style("Latch SDK version:", fg="bright_blue"),
            config.latch_version,
        ])
    )
    click.echo()

    with dest.open("w") as f:
        f.write("\n".join(get_prologue(config, wf_type)) + "\n\n")

        commands = infer_commands(pkg_root)
        if len(commands) > 0:
            click.echo()

        for block in commands:
            if block.order != DockerCmdBlockOrder.precopy:
                continue

            block.write_block(f)

        f.write("\n".join(copy_file_commands(wf_type)) + "\n\n")

        for block in commands:
            if block.order != DockerCmdBlockOrder.postcopy:
                continue

            block.write_block(f)

        f.write("\n".join(get_epilogue(wf_type)) + "\n")

    click.secho(f"Successfully generated dockerfile `{dest}`", fg="green")


def get_default_dockerfile(pkg_root: Path, *, wf_type: WorkflowType):
    default_dockerfile = pkg_root / "Dockerfile"

    if not default_dockerfile.exists():
        default_dockerfile = pkg_root / ".latch" / "Dockerfile"
        generate_dockerfile(
            pkg_root, dest=default_dockerfile, wf_type=wf_type, overwrite=True
        )

    return default_dockerfile
