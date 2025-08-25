from dataclasses import dataclass, field
from enum import Enum, auto
from io import TextIOWrapper
from pathlib import Path
from textwrap import dedent
from typing import List, Optional

import click
import yaml

from latch_cli.utils import WorkflowType
from latch_cli.workflow_config import (
    BaseImageOptions,
    LatchWorkflowConfig,
    get_or_create_workflow_config,
)


class DockerCmdBlockOrder(str, Enum):
    """Put a command block before or after the primary COPY command."""

    precopy = auto()
    copy = auto()
    postcopy = auto()


@dataclass(frozen=True)
class DockerCmdBlock:
    comment: str
    commands: List[str]
    order: DockerCmdBlockOrder

    def write_block(self, f: TextIOWrapper):
        f.write(f"# {self.comment}\n")
        f.write("\n".join(self.commands) + "\n\n")


@dataclass
class DockerfileBuilder:
    pkg_root: Path
    config: LatchWorkflowConfig
    wf_type: WorkflowType

    # todo(ayush): idk how i feel about mutable internal state here, refactor to make everything explicit perhaps?
    commands: List[DockerCmdBlock] = field(init=False, default_factory=list)

    apt_requirements: Optional[Path] = None
    r_env: Optional[Path] = None
    conda_env: Optional[Path] = None
    pyproject: Optional[Path] = None
    pip_requirements: Optional[Path] = None
    direnv: Optional[Path] = None

    def get_prologue(self):
        if self.wf_type == WorkflowType.snakemake:
            library_name = '"latch[snakemake]"'
        else:
            library_name = "latch"

        self.commands.append(
            DockerCmdBlock(
                comment="Prologue",
                commands=[
                    "# DO NOT CHANGE",
                    f"from {self.config.base_image}",
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
                    f"run pip install {library_name}=={self.config.latch_version}",
                    "run mkdir /opt/latch",
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    def get_epilogue(self):
        self.commands.append(
            DockerCmdBlock(
                comment="Epilogue",
                commands=[
                    "",
                    "# Latch workflow registration metadata",
                    "# DO NOT CHANGE",
                    "arg tag",
                    "# DO NOT CHANGE",
                    "env FLYTE_INTERNAL_IMAGE $tag",
                    "",
                    "workdir /root",
                ],
                order=DockerCmdBlockOrder.postcopy,
            )
        )

    def infer_apt_commands(self):
        if self.apt_requirements is None:
            return

        click.echo(
            " ".join([
                click.style(f"{self.apt_requirements.name}:", bold=True),
                "System dependencies installation phase",
            ])
        )

        self.commands.append(
            DockerCmdBlock(
                comment="Install system dependencies",
                commands=[
                    f"copy {self.apt_requirements} /opt/latch/system-requirements.txt",
                    dedent(r"""
                        run apt-get update --yes && \
                            xargs apt-get install --yes \
                                < /opt/latch/system-requirements.txt
                    """).strip(),
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    def infer_r_commands(self):
        if self.r_env is None:
            return

        click.echo(
            " ".join([
                click.style(f"{self.r_env.name}:", bold=True),
                "R dependencies installation phase",
            ])
        )

        # todo(maximsmol): allow specifying R version
        # todo(maximsmol): somehow promote using pak
        self.commands.extend([
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
                            """).strip()
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Install R",
                commands=["run rig add release # Change to any R version"],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Install R dependencies",
                commands=[
                    f"copy {self.r_env} /opt/latch/environment.R",
                    "run Rscript /opt/latch/environment.R",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
        ])

    def infer_conda_commands(self):
        if self.conda_env is None:
            return

        click.echo(
            " ".join([
                click.style(f"{self.conda_env.name}:", bold=True),
                "Conda dependencies installation phase",
            ])
        )

        with self.conda_env.open("rb") as f:
            env_content = yaml.safe_load(f)

        env_name = env_content.get("name", self.conda_env.stem)

        # todo(maximsmol): install `curl` and other build deps ahead of time once (or in base image)
        self.commands.extend([
            DockerCmdBlock(
                comment="Install Mambaforge",
                commands=[
                    dedent(r"""
                    run apt-get update --yes && \
                        apt-get install --yes curl git && \
                        curl \
                            --location \
                            --fail \
                            --remote-name \
                            https://github.com/conda-forge/miniforge/releases/latest/download/Miniforge3-Linux-x86_64.sh && \
                        `# Docs for -b and -p flags: https://docs.anaconda.com/anaconda/install/silent-mode/#linux-macos` \
                        bash Miniforge3-Linux-x86_64.sh -b -p /opt/conda -u && \
                        rm Miniforge3-Linux-x86_64.sh
                    """).strip()
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Set conda PATH",
                commands=[
                    "env PATH=/opt/conda/bin:$PATH",
                    "run conda config --set auto_activate_base false",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="Build conda environment",
                commands=[
                    f"copy {self.conda_env} /opt/latch/environment.yaml",
                    dedent(rf"""
                    run mamba env create \
                        --file /opt/latch/environment.yaml \
                        --name {env_name}
                    """).strip(),
                    f"env PATH=/opt/conda/envs/{env_name}/bin:$PATH",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
        ])

    def infer_installable_pyproject_commands(self):
        # from https://peps.python.org/pep-0518/ and https://peps.python.org/pep-0621/
        if self.pyproject is None:
            return

        rel = self.pyproject.resolve().relative_to(self.pkg_root.resolve())

        click.echo(
            " ".join([
                click.style(f"{self.pyproject.name}:", bold=True),
                "Python package installation phase",
            ])
        )

        self.commands.append(
            DockerCmdBlock(
                comment=f"Install python package defined by {self.pyproject}",
                commands=[f"run pip install /root/{rel.parent}"],
                order=DockerCmdBlockOrder.postcopy,
            )
        )

    def infer_pip_commands(self):
        if self.pip_requirements is None:
            return

        click.echo(
            " ".join([
                click.style(f"{self.pip_requirements.name}:", bold=True),
                "Python pip dependencies installation phase",
            ])
        )

        self.commands.append(
            DockerCmdBlock(
                comment=f"Install pip dependencies from `{self.pip_requirements}`",
                commands=[
                    f"copy {self.pip_requirements} /opt/latch/requirements.txt",
                    "run pip install --requirement /opt/latch/requirements.txt",
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    def infer_env_commands(self):
        if self.direnv is None:
            return

        click.echo(
            " ".join([
                click.style(f"{self.direnv.name}:", bold=True),
                "Environment variable setup",
            ])
        )
        envs: list[str] = []
        for line in self.direnv.read_text().splitlines():
            line = line.strip()

            if line == "" or line.startswith("#"):
                continue

            envs.append(f"env {line}")

        self.commands.append(
            DockerCmdBlock(
                comment="Set environment variables",
                commands=envs,
                order=DockerCmdBlockOrder.precopy,
            )
        )

    def infer_dependencies(self):
        self.infer_apt_commands()
        self.infer_r_commands()
        self.infer_conda_commands()
        self.infer_installable_pyproject_commands()
        self.infer_pip_commands()
        self.infer_env_commands()

    def get_copy_file_commands(self):
        cmd = ["copy . /root/"]

        if self.wf_type == WorkflowType.snakemake:
            cmd.extend([
                "",
                "# Latch snakemake workflow entrypoint",
                "# DO NOT CHANGE",
                "",
                "copy .latch/snakemake_jit_entrypoint.py /root/snakemake_jit_entrypoint.py",
            ])

        self.commands.append(
            DockerCmdBlock(
                comment="Copy workflow data (use .dockerignore to skip files)",
                commands=cmd,
                order=DockerCmdBlockOrder.copy,
            )
        )

    def generate(self, *, dest: Optional[Path] = None, overwrite: bool = False):
        if dest is None:
            dest = self.pkg_root / "Dockerfile"

        if (
            dest.exists()
            and not overwrite
            and not (
                click.confirm(f"Dockerfile already exists at `{dest}`. Overwrite?")
            )
        ):
            return

        click.secho("Generating Dockerfile", bold=True)

        click.echo(
            " ".join([
                click.style("Base image:", fg="bright_blue"),
                self.config.base_image,
            ])
        )
        click.echo(
            " ".join([
                click.style("Latch SDK version:", fg="bright_blue"),
                self.config.latch_version,
            ])
        )
        click.echo()

        self.get_prologue()
        self.infer_dependencies()
        self.get_copy_file_commands()
        self.get_epilogue()

        dockerfile_content: List[str] = []

        for order in [
            DockerCmdBlockOrder.precopy,
            DockerCmdBlockOrder.copy,
            DockerCmdBlockOrder.postcopy,
        ]:
            for command in self.commands:
                if command.order != order:
                    continue

                dockerfile_content.append(f"# {command.comment}")
                dockerfile_content.extend(command.commands)
                dockerfile_content.append("")

        dest.write_text("\n".join(dockerfile_content))

        click.secho(f"Successfully generated dockerfile `{dest}`", fg="green")


def generate_dockerignore(
    dest: Path, *, wf_type: WorkflowType, overwrite: bool = False
) -> None:
    if dest.exists():
        if dest.is_dir():
            click.secho(
                f".dockerignore already exists at `{dest}` and is a directory.",
                fg="red",
            )
            raise click.exceptions.Exit(1)

        if not overwrite and not click.confirm(
            f".dockerignore already exists at `{dest}`. Overwrite?"
        ):
            return

    files = [".git", ".github", ".venv"]

    if wf_type == WorkflowType.nextflow:
        files.extend([".nextflow*", ".nextflow.log*", "work/", "results/"])

    dest.write_text(dedent("\n".join(files) + "\n"))

    click.secho(f"Successfully generated .dockerignore `{dest}`", fg="green")


def get_default_dockerfile(
    pkg_root: Path, *, wf_type: WorkflowType, overwrite: bool = False
):
    default_dockerfile = pkg_root / "Dockerfile"

    config = get_or_create_workflow_config(
        pkg_root / ".latch" / "config",
        base_image_type=BaseImageOptions.nextflow
        if wf_type == WorkflowType.nextflow
        else BaseImageOptions.default,
    )

    if not default_dockerfile.exists():
        default_dockerfile = pkg_root / ".latch" / "Dockerfile"

        builder = DockerfileBuilder(pkg_root, config, wf_type)
        builder.generate(dest=default_dockerfile, overwrite=overwrite)

    return default_dockerfile
