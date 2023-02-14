# Dockerfiles per Task

When writing different tasks, defining distinct containers for each task
increases workflow performance by reducing the size of the container scheduled
on Kubernetes. It also helps with code organization by only associating
dependencies with the tasks that need them.

To use a separate Dockerfile for a task, pass the path of the Dockerfile when defining a task. If the workflow utilizes more than one Dockerfile, registration will take longer given that multiple containers must be built.

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

We can organize task definitions and Dockerfiles in a directory structure as follows:

```shell-session
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

`latch develop .` uses the Dockerfile in the workflow directory. In the future, we will support passing a Dockerfile as an argument to `latch develop`.
