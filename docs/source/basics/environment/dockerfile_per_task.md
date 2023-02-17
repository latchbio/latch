# Environments for Individual Tasks

Different tasks in a workflow may need different sets of dependencies. Creating a single shared environment can be problematic as the some part of the workflow image will be unused in each task and slow down that task's startup proportionally to the size of the extraneous chunk. Different dependencies might also need different system package versions in which case installing them together might be impractical.

Instead, consider defining an individual environment for each task using the optional `dockerfile` parameter in the task definition. Include only the dependencies that each specific task needs.

```python
# assemble.py

from pathlib import Path

from latch import small_task
from latch.types import LatchFile

# Path relative to the task directory

@small_task(dockerfile=Path(__file__).parent / "Dockerfile")
def assembly_task(read1: LatchFile, read2: LatchFile) -> LatchFile:
    ...
    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")
```

```python
# sam_blaster.py

from pathlib import Path

from latch import small_task
from latch.types import LatchFile

@small_task(dockerfile=Path(__file__).parent / "Dockerfile")
def sam_blaster(sam: LatchFile) -> LatchFile:
    ...
    return LatchFile(blasted_sam, f"latch:///{blasted_sam.name}")
```

`Dockerfile`s can be organized as follows:

```shell-session
wf
├── Dockerfile
├── __init__.py
├── assemble
│   ├── Dockerfile
│   └── __init__.py
└── sam_blaster
    ├── Dockerfile
    └── __init__.py
```

The root directory used when building the images is always the workflow directory.

## Limitations

`latch develop` uses the `Dockerfile` in the workflow directory and not any of the individual `Dockerfiles`
