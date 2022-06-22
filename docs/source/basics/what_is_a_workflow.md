# What is a Workflow?

A workflow is an analysis that takes in some input, processes it in one or more
steps and produces some output.

---

Formally, a workflow can be described as a [directed acyclic graph](https://en.wikipedia.org/wiki/Directed_acyclic_graph) (DAG), where each
node in the graph is called a task. This computational graph is a flexible model
to describe most any bioinformatics analysis.

In this example, a workflow ingests sequencing files in FastQ format and
produces a sorted assembly file. The workflow's DAG has two tasks. The first
task turns the FastQ files into a single BAM file using an assembly algorithm.
The second task sorts the assembly from the first task. The final output is a
useful assembly conducive to downstream analysis and visualization in tools like
[IGV](https://software.broadinstitute.org/software/igv/).

The Latch SDK lets you define your workflow tasks as python functions.
The parameters in the function signature define the task inputs and return
values define the task outputs. The body of the function holds the task logic,
which can be written in plain python or can be subprocessed through a
program/library in any language.

```python
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
```

These tasks are then "glued" together in another function that represents the
workflow. The workflow function body simply chains the task functions by calling
them and passing returned values to downstream task functions. Notice that our
workflow function calls the task that we just defined, `assembly_task`, as well
as another task we can assume was defined elsewhere, `sort_bam_task`.

You must not write actual logic in the workflow function body. It can only be
used to call task functions and pass task function return values to downstream
task functions. Additionally all task functions must be called with keyword
arguments.

```python
@workflow
def assemble_and_sort(read1: LatchFile, read2: LatchFile) -> LatchFile:

    sam = assembly_task(read1=read1, read2=read2)
    return sort_bam_task(sam=sam)
```

Workflow function docstrings also contain markdown formatted
documentation and a DSL to specify the presentation of parameters when the
workflow interface is generated. We'll add this content to the docstring of the
workflow function we just wrote.

```python
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

    __metadata__:
        display_name: Assemble and Sort FastQ Files
        author:
            name:
            email:
            github:
        repository:
        license:
            id: MIT

    Args:

        read1:
          Paired-end read 1 file to be assembled.

          __metadata__:
            display_name: Read1

        read2:
          Paired-end read 2 file to be assembled.

          __metadata__:
            display_name: Read2
    """

    sam = assembly_task(read1=read1, read2=read2)
    return sort_bam_task(sam=sam)
```

## Workflow Code Structure

So far we have defined workflows and tasks as python functions but we don't know
where to put them or what supplementary files might be needed to run the code on
the Latch platform.

Workflow code needs to live in directory with three necessary
elements:

* a file named `Dockerfile` that defines the computing environment of your tasks
* a file named `version` that holds the plaintext version of the workflow
* a directory named `wf` that holds the python code needed for the workflow.
* task and workflow functions must live in a `wf/__init__.py` file

These three elements must be named as specified above. The directory should have
the following structure:

```text
├── Dockerfile
├── version
└── wf
    └── __init__.py
```

The SDK ships with easily retrievable example workflow code. Just type
`latch init myworkflow` to construct a directory structured as above for
reference or boilerplate.

### Example `Dockerfile`

**Note**: you are required to use our base image for the time being.

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
RUN  sed -i 's/latch/wf/g' flytekit.config
RUN python3 -m pip install --upgrade latch
WORKDIR /root
```

### Example `version` File

You can use any versioning scheme that you would like, as long as each register
has a unique version value. We recommend sticking with [semantic
versioning](https://semver.org/).

```text
v0.0.0
```

### Example `wf/__init__.py` File

```python
import subprocess
from pathlib import Path

from latch import small_task, workflow
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


@small_task
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

    __metadata__:
        display_name: Assemble and Sort FastQ Files
        author:
            name:
            email:
            github:
        repository:
        license:
            id: MIT

    Args:

        read1:
          Paired-end read 1 file to be assembled.

          __metadata__:
            display_name: Read1

        read2:
          Paired-end read 2 file to be assembled.

          __metadata__:
            display_name: Read2
    """
    sam = assembly_task(read1=read1, read2=read2)
    return sort_bam_task(sam=sam)
```

## What happens at registration?

Now that we've defined our functions, we are ready to register our workflow with
the [LatchBio](https://latch.bio) platform. This will give us:

* a no-code interface
* managed cloud infrastructure for workflow execution
* a dedicated API endpoint for programmatic execution
* hosted documentation
* parallelized CSV-to-batch execution

To register, we type `latch register <directory_name>` into our terminal (where
directory_name is the name of the directory holding our code, Dockerfile and
version file).

The registration process requires a local installation of Docker.

To re-register changes, make sure you update the value in the version file. (The
value of the version is not important, only that it is distinct from previously
registered versions).
