<div align="center">

![biocompute](static/biocompute.png)

# Latch SDK

The Latch SDK is an open-source toolchain to define serverless bioinformatics
workflows with plain python and deploy associated no-code interfaces with
single command.

With the Latch SDK, developers are able to write workflows as python functions
and dynamically compile type-safe and highly customizable web apps to execute
this logic. Workflow code is serialized, containerized and versioned
transiently for maximum reproducibility and portability. Workflows can also be
annotated with arbitrary resource constraints, eg. cores, RAM, GPUs, of which
Latch can satisfy at execution time.

It is built directly on [Flyte](https://docs.flyte.org) for all the benefits that the Kubernetes-native
workflow orchestration framework provides - task-level type-safety and
containerization, independent task scheduling, and heterogeneous & highly
scalable computing infrastructure.

[Docs](https://docs.latch.bio) •
[Installation](#installation) •
[Quickstart](#configuration) •
[Latch](https://latch.bio)


![side-by-side](static/side-by-side.png)

</div>

## Installation

```sh
$ pip install latch
```

_Make sure you install [docker](https://docs.docker.com/) for your machine! You will be building lots of containers..._

## Quickstart

Initialize workflow boilerplate and register the workflow with Latch in 60 seconds.

```
$ latch init test-workflow
$ latch register test-workflow
```

Copy local files to latch data.

```
$ latch cp test.fa latch:///samples/test.fa
```

## More Examples

Vist our official [docs](https://docs.latch.bio) for tutorials, workflow
recipes and full API specifications.

Ingest TBs of genomic files and call arbitrary programs...

```
from latch import task
from latch.types import LatchDir, BamFile

@task()
def samtools_sort_tsk(
    bam_file: BamFile,
    output_dir: LatchDir= LatchDir("latch://sorted_output"),
) -> FlyteDirectory:

    local_output = Path("/root/output")

    _cmd = [
        "samtools",
        "sort",
        str(fasta_file),
        "-o",
        str(local_output),
    ]

    subprocess.run(_cmd, check=True)

    return LatchDirectory(local_output)
```

Scale your logic to curated high-performance and GPU-enabled computing instances with a single line...

```
from latch.tasks import large_gpu_task
from latch.types import LatchDir, LatchFile

@large_gpu_task() # 4 GPU, 32 CPU, 256 RAM
def train_protein_model(samples: LatchDir) -> LatchFile:
  ...
```


<div align="center">

## carpe diem

![manske](static/manske.png)

</div>
