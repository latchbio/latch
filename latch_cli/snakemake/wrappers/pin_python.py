import sys
from pathlib import Path
from re import A
from typing import Any, Dict, List, Union

import yaml


def pin_python(env_file: Path):
    content: Dict[str, Any] = yaml.safe_load(env_file.read_text())

    new_chans = []
    if "channels" in content:
        new_chans: List[str] = content["channels"]

    if "bioconda" not in new_chans:
        new_chans.append("bioconda")

    all_deps = []
    if "dependencies" in content:
        all_deps: List[Union[str, Dict[str, List[str]]]] = content["dependencies"]

    conda_deps: List[str] = []
    pip_deps: List[str] = []
    for dep in all_deps:
        if isinstance(dep, str):
            conda_deps.append(dep)
            continue

        for pip_dep in dep["pip"]:
            pip_deps.append(pip_dep)

    pip_deps.append("latch[snakemake]")

    conda_deps.append(f"python>=3.7,<3.12")
    conda_deps.append("snakemake-wrapper-utils")

    new_deps = [*conda_deps, {"pip": pip_deps}]

    content["dependencies"] = new_deps
    content["channels"] = new_chans

    print(yaml.safe_dump(content))
    env_file.write_text(yaml.safe_dump(content))


if __name__ == "__main__":
    if len(sys.argv) == 1:
        print("No environment file passed - defaulting to environment.yaml")
        env_file = Path("environment.yaml")
    else:
        env_file = Path(sys.argv[1])

    pin_python(env_file)
