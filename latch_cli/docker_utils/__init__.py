import functools
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from textwrap import dedent
from typing import List

import yaml

from latch_cli.types import LatchWorkflowConfig

print = functools.partial(print, flush=True)


class DockerCmdBlockOrder(Enum):
    FIRST = auto()
    PRECOPY = auto()
    POSTCOPY = auto()
    LAST = auto()


@dataclass
class DockerCmdBlock:
    comment: str
    commands: List[str]
    order: DockerCmdBlockOrder


def get_prologue(config: LatchWorkflowConfig) -> List[str]:
    return [
        "# latch base image + dependencies for latch SDK -- removing these will break the workflow",
        f"from {config.base_image}",
        f"run python3 -m pip install latch=={config.latch_version}",
    ]


def get_epilogue() -> List[str]:
    return [
        "# latch internal tagging system + expected root directory -- changing these lines will break the workflow",
        "arg tag",
        "env FLYTE_INTERNAL_IMAGE $tag",
        "WORKDIR /root",
    ]


def infer_commands(pkg_root: Path) -> List[DockerCmdBlock]:
    commands: List[DockerCmdBlock] = []

    # apt requirements in workflow directory
    if (pkg_root / "requirements.apt").exists():
        print("Install apt requirements from `requirements.apt`")
        commands.append(
            DockerCmdBlock(
                comment="install apt requirements",
                commands=[
                    "copy requirements.apt /root/requirements.apt",
                    "run apt-get update -y && xargs apt-get install -y </root/requirements.apt",
                ],
                order=DockerCmdBlockOrder.PRECOPY,
            )
        )

    # R requirements in workflow directory
    if (pkg_root / "environment.R").exists():
        print("Install R packages from `environment.R`y")
        commands.append(
            DockerCmdBlock(
                comment="install R requirements",
                commands=[
                    dedent(
                        """\
                        run apt-get update -y && \\
                            apt-get install -y software-properties-common && \\
                            add-apt-repository "deb http://cloud.r-project.org/bin/linux/debian buster-cran40/" && \\
                            apt-get install -y r-base r-base-dev libxml2-dev libcurl4-openssl-dev libssl-dev wget \
                        """
                    ).strip(),
                    "copy environment.R /root/environment.R",
                    "run Rscript /root/environment.R",
                ],
                order=DockerCmdBlockOrder.PRECOPY,
            )
        )

    # conda environment in workflow directory
    if (pkg_root / "environment.yml").exists():
        print("Create conda environment from `environment.yml`")
        with open(pkg_root / "environment.yml") as f:
            conda_env = yaml.safe_load(f.read())
            if "name" in conda_env:
                env_name = conda_env["name"]
            else:
                env_name = "unnamed"

        commands += [
            DockerCmdBlock(
                comment="Set conda environment variables",
                commands=[
                    "env CONDA_DIR /opt/conda",
                    "env PATH=$CONDA_DIR/bin:$PATH",
                ],
                order=DockerCmdBlockOrder.PRECOPY,
            ),
            DockerCmdBlock(
                comment="install miniconda",
                commands=[
                    dedent(
                        """\
                        run apt-get update -y && \\
                            apt-get install -y curl && \\
                            curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \\
                            mkdir /root/.conda && \\
                            bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \\
                            rm -f Miniconda3-latest-Linux-x86_64.sh && \\
                            conda init bash \
                        """
                    ).strip(),
                ],
                order=DockerCmdBlockOrder.PRECOPY,
            ),
            DockerCmdBlock(
                comment="build and configure conda environment",
                commands=[
                    "copy environment.yml /root/environment.yml",
                    f"run conda env create -f environment.yml --name {env_name}",
                    f"""shell ["conda", "run", "-n", "{env_name}", "/bin/bash", "-c"]""",
                    "run /opt/conda/bin/pip install --upgrade latch",
                ],
                order=DockerCmdBlockOrder.PRECOPY,
            ),
        ]

    # python package in workflow directory (poetry, distutils, etc.)
    if (pkg_root / "setup.py").exists() or (pkg_root / "pyproject.toml").exists():
        print("Add local package to python environment")
        commands.append(
            DockerCmdBlock(
                comment="add local package to python environment",
                commands=["run python3 -m pip install -e /root/"],
                order=DockerCmdBlockOrder.POSTCOPY,
            )
        )

    # python requirements.txt
    if (pkg_root / "requirements.txt").exists():
        print("Add requirements from `requirements.txt` to python environment")
        commands.append(
            DockerCmdBlock(
                comment="add requirements from `requirements.txt` to python environment",
                commands=[
                    "copy requirements.txt /root/requirements.txt",
                    "run python3 -m pip install -r requirements.txt",
                ],
                order=DockerCmdBlockOrder.PRECOPY,
            )
        )

    return commands


def generate_dockerfile(pkg_root: Path, outfile: Path) -> None:
    """
    Generate a Dockerfile from files in the workflow directory.
    """

    print("Generating Dockerfile")
    try:
        with open(pkg_root / ".latch") as f:
            config: LatchWorkflowConfig = LatchWorkflowConfig.from_json(f.read())
            print("  - base image:", config.base_image)
            print("  - latch version:", config.latch_version)
    except FileNotFoundError:
        raise RuntimeError(
            "Could not find .latch file in supplied directory. If your workflow was created previously to release 2.13.0, you may need to run `latch init` to generate a .latch file."
        )

    with open(outfile, "w") as f:
        f.write("\n".join(get_prologue(config)) + "\n\n")

        commands = infer_commands(pkg_root)
        pre_commands = [c for c in commands if c.order == DockerCmdBlockOrder.PRECOPY]
        post_commands = [c for c in commands if c.order == DockerCmdBlockOrder.POSTCOPY]

        for block in pre_commands:
            f.write(f"# {block.comment}\n")
            f.writelines("\n".join(block.commands) + "\n\n")

        f.write("# copy all code from package (use .latchignore to skip files)\n")
        f.write("copy . /root/\n\n")

        for block in post_commands:
            f.write(f"# {block.comment}\n")
            f.writelines("\n".join(block.commands) + "\n\n")

        f.writelines("\n".join(get_epilogue()) + "\n")

    print("Generated.")
