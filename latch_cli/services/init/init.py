"""Service to initialize boilerplate."""

import os
import re
import shutil
from enum import Enum, auto
from pathlib import Path
from textwrap import dedent
from typing import Optional


class _Templates(Enum):
    default = auto()
    r = auto()
    conda = auto()


def init(pkg_name: Path, template: Optional[str] = None):
    """Creates boilerplate workflow files in the user's working directory.

    Args:
        pkg_name: A identifier for the workflow - will name the boilerplate
            directory as well as functions within the constructed package.
        template: Which (if any) template to use when generating the files.
            Valid choices are 'default', 'r', or 'conda'. If `template` is None,
            the package will be generated using the 'default' template.

    Example:

        >>> init("test-workflow")
            # The resulting file structure will look like
            #   test-workflow
            #   ├── Dockerfile
            #   ├── reference
            #   │   ├── wuhan.1.bt2
            #   │   ├── wuhan.2.bt2
            #   │   ├── wuhan.3.bt2
            #   │   ├── wuhan.4.bt2
            #   │   ├── wuhan.fasta
            #   │   ├── wuhan.rev.1.bt2
            #   │   └── wuhan.rev.2.bt2
            #   ├── version
            #   └── wf
            #       └── __init__.py

    """
    # click doesn't support enums for options hence '.name' madness

    # Workflow name must not contain capitals or end in a hyphen or underscore. If it does, we should throw an error
    # Check for capitals
    if any(char.isupper() for char in str(pkg_name)):
        raise ValueError(
            f"Unable to initialize {str(pkg_name)}: package name must not contain any"
            " upper-case characters",
        )

    # Check for other illegal characters
    if (
        len(
            re.findall(
                "(?:[a-z0-9]+(?:[._-][a-z0-9]+)*\/)*[a-z0-9]+(?:[._-][a-z0-9]+)*",
                pkg_name,
            )
        )
        != 1
    ):
        raise ValueError(
            dedent(
                f"""
                Unable to initialize {pkg_name}: package name must match the regular
                expression

                    `(?:[a-z0-9]+(?:[._-][a-z0-9]+)*\/)*[a-z0-9]+(?:[._-][a-z0-9]+)*`

                This means that the package name must start and end with a
                lower-case letter, and may only contain hyphens, underscores, and
                periods,
                """
            )
        )

    if template is None:
        template = _Templates.default.name

    if template not in [t.name for t in _Templates]:
        raise ValueError(
            f"Invalid template name. valid options are {[t.name for t in _Templates]}"
        )

    cwd = Path(os.getcwd()).resolve()
    pkg_root = cwd / pkg_name
    try:
        pkg_root.mkdir(parents=True)
    except FileExistsError:
        raise OSError(
            f"A directory of name {pkg_name} already exists."
            " Remove it or pick another name for your latch workflow."
        )

    if template == _Templates.default.name:
        _gen_assemble_and_sort(pkg_root)
    elif template == _Templates.r.name:
        _gen_example_r(pkg_root)
    elif template == _Templates.conda.name:
        _gen_example_conda(pkg_root)
    else:
        raise ValueError(
            f"Invalid template name. valid options are {[t.name for t in _Templates]}"
        )


def _get_boilerplate(pkg_root: Path, source_path: Path):
    pkg_root = pkg_root.resolve()
    source_path = source_path.resolve()

    wf_root = pkg_root / "wf"
    wf_root.mkdir(exist_ok=True)
    init_f = wf_root / "__init__.py"
    init_source = source_path / "__init__.py"
    shutil.copy(init_source, init_f)

    version_f = pkg_root / "version"
    with open(version_f, "w") as f:
        f.write("0.0.0")

    docker_f = pkg_root / "Dockerfile"
    docker_source = source_path / "Dockerfile"
    shutil.copy(docker_source, docker_f)


def _gen_assemble_and_sort(pkg_root: Path):
    import boto3
    from botocore import UNSIGNED
    from botocore.config import Config

    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "assemble_and_sort"

    _get_boilerplate(pkg_root, source_path)

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


def _gen_example_r(pkg_root: Path):
    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "example_r"

    _get_boilerplate(pkg_root, source_path)


def _gen_example_conda(pkg_root: Path):
    pkg_root = pkg_root.resolve()
    source_path = Path(__file__).parent / "example_conda"

    _get_boilerplate(pkg_root, source_path)

    imports_dest = pkg_root / "requirements.txt"
    imports_source = source_path / "requirements.txt"
    shutil.copy(imports_source, imports_dest)
