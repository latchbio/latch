import json
from dataclasses import dataclass
from enum import Enum, auto
from io import TextIOWrapper
from pathlib import Path
from textwrap import dedent
from typing import List

import yaml

from latch_cli.constants import latch_constants
from latch_cli.workflow_config import LatchWorkflowConfig, create_and_write_config


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


def get_prologue(config: LatchWorkflowConfig) -> List[str]:
    return [
        (
            "# latch base image + dependencies for latch SDK --- removing these will"
            " break the workflow"
        ),
        f"from {config.base_image}",
        f"run pip install latch=={config.latch_version}",
        f"run mkdir /opt/latch",
    ]


def get_epilogue() -> List[str]:
    return [
        (
            "# latch internal tagging system + expected root directory --- changing"
            " these lines will break the workflow"
        ),
        "arg tag",
        "env FLYTE_INTERNAL_IMAGE $tag",
        "workdir /root",
    ]


def infer_commands(pkg_root: Path) -> List[DockerCmdBlock]:
    commands: List[DockerCmdBlock] = []

    if (pkg_root / "system-requirements.txt").exists():
        print("Adding system requirements installation phase")
        commands.append(
            DockerCmdBlock(
                comment="install system requirements",
                commands=[
                    "copy system-requirements.txt /opt/latch/system-requirements.txt",
                    (
                        "run apt-get update --yes && xargs apt-get install --yes"
                        " </opt/latch/system-requirements.txt"
                    ),
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    if (pkg_root / "environment.R").exists():
        print("Adding R + R package installation phase")
        commands.append(
            DockerCmdBlock(
                comment="install R requirements",
                commands=[
                    dedent("""
                        run apt-get update --yes && \\
                            apt-get install --yes software-properties-common && \\
                            add-apt-repository "deb http://cloud.r-project.org/bin/linux/debian buster-cran40/" && \\
                            DEBIAN_FRONTEND=noninteractive apt-get install --yes r-base r-base-dev libxml2-dev libcurl4-openssl-dev libssl-dev wget
                        """).strip(),
                    "copy environment.R /opt/latch/environment.R",
                    "run Rscript /opt/latch/environment.R",
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    if (pkg_root / "environment.yaml").exists():
        print("Adding conda + conda environment installation phase")
        with (pkg_root / "environment.yaml").open("rb") as f:
            conda_env = yaml.safe_load(f)

        if "name" in conda_env:
            env_name = conda_env["name"]
        else:
            env_name = "workflow"

        commands += [
            DockerCmdBlock(
                comment="set conda environment variables",
                commands=[
                    "env CONDA_DIR /opt/conda",
                    "env PATH=$CONDA_DIR/bin:$PATH",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="install miniconda",
                commands=[
                    dedent("""
                        run apt-get update --yes && \\
                            apt-get install --yes curl && \\
                            curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \\
                            mkdir /root/.conda && \\
                            # docs for -b and -p flags: https://docs.anaconda.com/anaconda/install/silent-mode/#linux-macos
                            bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \\
                            rm -f Miniconda3-latest-Linux-x86_64.sh
                        """).strip(),
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
            DockerCmdBlock(
                comment="build and configure conda environment",
                commands=[
                    "copy environment.yaml /opt/latch/environment.yaml",
                    (
                        "run conda env create --file /opt/latch/environment.yaml"
                        f" --name {env_name}"
                    ),
                    f"env PATH=$CONDA_DIR/envs/{env_name}/bin:$PATH",
                ],
                order=DockerCmdBlockOrder.precopy,
            ),
        ]

    # from https://peps.python.org/pep-0518/ and https://peps.python.org/pep-0621/
    if (pkg_root / "setup.py").exists() or (pkg_root / "pyproject.toml").exists():
        print("Adding local package installation phase")
        commands.append(
            DockerCmdBlock(
                comment="add local package to python environment",
                commands=["run pip install --editable /root/"],
                order=DockerCmdBlockOrder.postcopy,
            )
        )

    if (pkg_root / "requirements.txt").exists():
        print("Adding python dependency installation phase")
        commands.append(
            DockerCmdBlock(
                comment=(
                    "add requirements from `requirements.txt` to python environment"
                ),
                commands=[
                    "copy requirements.txt /opt/latch/requirements.txt",
                    "run pip install --requirement /opt/latch/requirements.txt",
                ],
                order=DockerCmdBlockOrder.precopy,
            )
        )

    if (pkg_root / ".env").exists():
        print("Adding environment variable phase")
        envs = []
        for line in (pkg_root / ".env").read_text().splitlines():
            if line.startswith("#"):
                continue
            if line.strip() == "":
                continue
            envs.append(f"env {line}")

        commands.append(
            DockerCmdBlock(
                comment="set environment variables",
                commands=envs,
                order=DockerCmdBlockOrder.postcopy,
            )
        )

    return commands


def generate_dockerfile(pkg_root: Path, outfile: Path) -> None:
    """Generate a best effort Dockerfile from files in the workflow directory.

    Args:
        pkg_root: A path to a workflow directory.
        outfile: The path to write the generated Dockerfile.

    Example:

        >>> generate_dockerfile(Path("test-workflow"), Path("test-workflow/Dockerfile"))
            # The resulting file structure will look like
            #   test-workflow
            #   ├── Dockerfile
            #   └── ...
    """

    print("Generating Dockerfile")
    try:
        with (pkg_root / latch_constants.pkg_config).open("r") as f:
            config: LatchWorkflowConfig = LatchWorkflowConfig(**json.load(f))
            print("  - base image:", config.base_image)
            print("  - latch version:", config.latch_version)
    except FileNotFoundError as e:
        print(
            "Could not find a .latch/config file in the supplied directory. Creating"
            " configuration"
        )
        create_and_write_config(pkg_root)
        with (pkg_root / latch_constants.pkg_config).open("r") as f:
            config: LatchWorkflowConfig = LatchWorkflowConfig(**json.load(f))

    with outfile.open("w") as f:
        f.write("\n".join(get_prologue(config)) + "\n\n")

        commands = infer_commands(pkg_root)

        for block in commands:
            if block.order == DockerCmdBlockOrder.precopy:
                block.write_block(f)

        f.write("# copy all code from package (use .dockerignore to skip files)\n")
        f.write("copy . /root/\n\n")

        for block in commands:
            if block.order == DockerCmdBlockOrder.postcopy:
                block.write_block(f)

        f.write("\n".join(get_epilogue()) + "\n")

    print("Generated.")
