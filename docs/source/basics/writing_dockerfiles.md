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
FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:dd8f-main

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

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
RUN python3 -m pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
```

_Note that we must use the base image specified in the first line to configure
libraries and settings for consistent task behavior._

There are 3 latch-base images available as seen on [github](https://github.com/latchbio/latch-base).

## Writing Dockerfiles for different tasks

When writing different tasks, defining distinct containers for each task
increases workflow performance by reducing the size of the container scheduled
on Kubernetes. It also helps with code organization by only associating
dependencies with the tasks that need them.

You can write different Dockerfiles and associate them to tasks by passing
their paths to the task definition. When registering a workflow with multiple
containers, note that each container will be rebuilt, which can slow down
development.

```
@small_task(dockerfile=Path(__file__).parent.parent / "DockerfileMultiQC")
def sample_task(int: a) -> str:
    return str(a)
```

### An example for task-specific Dockerfiles

Since our default assemble and sort workflow has two tasks,
let's split this workflow's original Dockerfile in two - making one Dockerfile per task -
to show how you can build a pipeline containing task-specific Dockerfiles.

First, let's create the workflow directory we'll be working in:

```
latch init dockerfile-per-task
```

Then, we'll build the assembly task and its respective Dockerfile.

```
# wf/assembly/__init__.py

import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile

# Note the use of paths relative to the location of the __init__.py file
@small_task(dockerfile=Path(__file__).parent / "Dockerfile")
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
```

Note, in the python script above, the use of relative paths - we're using `Path(__file__).parent / "Dockerfile"` to specify where this task's Dockerfile can be found.

`Path(__file__)` means the path to the current Python script (`wf/assembly/__init__.py`)
and `.parent` means the parent directory of this script (`wf/assembly/`).
The final path then being `wf/assembly/Dockerfile`.

For this workflow, we're keeping the Dockerfiles for each task
in the same directory where the task definition for that task is located.
Though we recommend keeping this same structure, you are free to change the
location of these files, as long as you also change the `dockerfile` argument
of each task.

Now, let's build the Dockerfile itself.
Since the assembly task requires `bowtie2`, this first Dockerfile
can look like this:

```Dockerfile
# wf/assembly/Dockerfile

FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:dd8f-main

# Install curl and unzip
# Both necessary to install bowtie2 (See the next RUN command)
RUN apt-get update -y && \
    apt-get install -y curl unzip

# Install bowtie2
RUN curl -L \
    https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.4.4/bowtie2-2.4.4-linux-x86_64.zip/download \
    -o bowtie2-2.4.4.zip && \
    unzip bowtie2-2.4.4.zip && \
    mv bowtie2-2.4.4-linux-x86_64 bowtie2

# Copy reference
COPY reference /root/reference
ENV BOWTIE2_INDEXES="reference"

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
RUN python3 -m pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
```

Notice how you still have to use the same base image at the top of each Dockerfile
and keep the latch installation commands at the bottom,
as shown above.

Now, let's build the sorting task and its respective dockerfile:

```
# wf/sort/__init__.py

import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile


@small_task(dockerfile=Path(__file__).parent / "Dockerfile")
def sort_bam_task(sam: LatchFile) -> LatchFile:

    bam_file = Path("covid_sorted.bam").resolve()

    _samtools_sort_cmd = [
        "samtools",
        "sort",
        "-o",
        str(bam_file),
        "-O",
        "bam",
        sam.local_path,
    ]

    subprocess.run(_samtools_sort_cmd)

    return LatchFile(str(bam_file), "latch:///covid_sorted.bam")
```

Since this task requires `samtools`, this Dockerfile looks like this:

```Dockerfile
# wf/sort/Dockerfile

FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:dd8f-main

# Install samtools
RUN apt-get install -y autoconf samtools

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
RUN python3 -m pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
```

Finally, the workflow itself can look something like this:

```
from latch import workflow
from latch.types import LatchFile

from wf.assembly import assembly_task
from wf.sort import sort_bam_task

@workflow
def assemble_and_sort(read1: LatchFile, read2: LatchFile) -> LatchFile:
    """Description...

    markdown header
    ----

    Write some documentation about your workflow in
    markdown here:

    > Regular markdown constructs work as expected.

    # Heading

    * content1
    * content2
    """
    sam = assembly_task(read1=read1, read2=read2)
    return sort_bam_task(sam=sam)
```

Notice how we can organize task definitions and Docker containers into their
own directories:

```
├── Dockerfile
├── version
└── wf
    ├── __init__.py
    ├── assembly
    │   ├── Dockerfile
    │   └── __init__.py
    └── sort
        ├── Dockerfile
        └── __init__.py
```

We recommend keeping this same basic structure for other workflows with
task-specific Dockerfiles, since it leads to better modularity and code
organization, making it easy to see which task requires which Dockerfile.
And also facilitating for you or other users to re-use these task
definitions in other workflows.

Note how you still have to include a Dockerfile at the root of your workflow's directory.
This file can contain just the basic boilerplate for SDK workflows, that is:

```Dockerfile
FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:dd8f-main

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
RUN python3 -m pip install --upgrade latch
COPY wf /root/wf
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
WORKDIR /root
```

## Limitations

The difference between a latch task environment and running your code on your
Linux machine is that we restrict your access to `/dev/` and the networking
stack. For example, you cannot create mounts using `/dev/fuse` (so mounts are
generally off limits) and you do not have admin access to the networking stack
on the host machine as your task execution does not have root access.

The remote development utilities (`latch develop .`) do not work with
different containers per task and we do not recommend using the two features
together for the time being.
