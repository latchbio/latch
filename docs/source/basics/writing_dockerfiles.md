# Writing Dockerfiles

Dockerfiles allow you to define the computing environment that your task will
execute in.

To write a Dockerfile, you simply write the commands that you want executed and
specify the files that you want available before your task is run. A Dockerfile
defines your task's "image" (the recipe for its virtual computing environment)
which will become a container that it will execute within at runtime.

---

Here is an example of a Dockerfile used earlier:

```Dockerfile
FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:9a7d-main

# Its easy to build binaries from source that you can later reference as
# subprocesses within your workflow.
RUN curl -L https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.4.4/bowtie2-2.4.4-linux-x86_64.zip/download -o bowtie2-2.4.4.zip &&\
    unzip bowtie2-2.4.4.zip &&\
    mv bowtie2-2.4.4-linux-x86_64 bowtie2

# Or use managed library distributions through the container OS's package
# manager.
RUN apt-get update -y &&\
    apt-get install -y autoconf samtools


# You can use local data to construct your workflow image.  Here we copy a
# pre-indexed reference to a path that our workflow can reference.
COPY data /root/reference
ENV BOWTIE2_INDEXES="reference"

COPY wf /root/wf

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
RUN sed -i 's/latch/wf/g' flytekit.config
RUN python3 -m pip install --upgrade latch
WORKDIR /root
```

_Note that we must use the base image specified in the first line to configure
libraries and settings for consistent task behavior._

There are 3 latch-base images available as seen on [github](https://github.com/latchbio/latch-base). 

## Writing Dockerfiles for different tasks

When writing different tasks, defining distinct containers for each task
increases workflow performance by reducing the size of the container scheduled
on kubernetes. It also helps with code organization by only associating dependency
installation code with the task that strictly needs it.

You can write different Dockerfiles and associate them to tasks by passing
their paths to the task definition. When registering a workflow with multiple
containers, it is important to realize that each container will be built with
every registration, which could slow development. 

```
@small_task(dockerfile=Path(__file__).parent.parent / "DockerfileMultiQC")
```

A full example:

```
import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile


@small_task
def assembly_task(read1: LatchFile, read2: LatchFile) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    _bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "-x",
        "wuhan",
        "-1",
        read1.local_path,
        "-2",
        read2.local_path,
        "--very-sensitive-local",
        "-S",
        str(sam_file),
    ]

    subprocess.run(_bowtie2_cmd)

    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")


# Note the use of paths relative to the location of the __init__.py file

@small_task(dockerfile=Path(__file__).parent.parent / "DockerfileBlaster")
def sam_blaster(sam: LatchFile) -> LatchFile:

    blasted_sam = Path(sam.name + ".blasted.sam")

    subprocess.run(
        ["samblaster", "-i", sam.local_path, "-o", str(blasted_sam.resolve())]
    )

    return LatchFile(blasted_sam, f"latch:///{blasted_sam.name}")
```

```
├── Dockerfile
├── DockerfileBlaster
├── version
└── wf
    ├── __init__.py
```

The remote development utilities (`latch develop .`) do not work with
different containers per task and we do not recommend using the two features
together for the time being.


## Limitations

The difference between a latch task environment and running your code on your
Linux machine is that we restrict your access to `/dev/` and the networking
stack.  For example, you cannot create mounts using `/dev/fuse` (so mounts are
generally off limits) and you do not have admin access to the networking stack
on the host machine as your task execution does not have root access.
