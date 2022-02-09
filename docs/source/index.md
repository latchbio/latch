# Latch SDK Documentation

The Latch SDK is an open-source toolchain to define serverless bioinformatics
workflows with plain python and deploy associated no-code
interfaces with single command.

With the Latch SDK, developers are able to write workflows as python functions
and dynamically compile type-safe and highly customizable web apps to execute
this logic. Workflow code is serialized, containerized and versioned transiently
for maximum reproducibility and portability. Workflows can also be annotated
with arbitrary resource constraints, eg. cores, RAM, GPUs, of which Latch can
satisfy at execution time.

It is built directly on [Flyte](https://docs.flyte.org) for all the benefits that the
Kubernetes-native workflow orchestration framework provides - task-level
type-safety and containerization, independent task scheduling, and heterogeneous
& highly scalable computing infrastructure.

## Quickstart

```
pip install latch
```

_You will need to install and run docker locally to register workflows with
latch. Download [docker for mac](https://docs.docker.com/engine/install/) or
[docker for linux](https://www.docker.com/products/docker-desktop)_

### 60 Second Workflow

_Again make sure docker is running!_

```
latch init myworkflow
latch register myworkflow
```

`latch init` will give us some minimal boilerplate that we can immediately
begin playing with. Note that when we first invoke `latch register`, we will be
redirected to sign into the Latch platform through the browser.

Our toy workflow will be available at [console.latch.bio](console.latch.bio)
after the dialogue in your terminal has completed.

There is much left to do. You can ingest enormous files and execute custom
programs written in any language... 

```
@task()
def samtools_sort_tsk(
    bam_file: BamFile,
    output_dir: LatchDirectory = LatchDirectory("latch://sorted_output"),
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

scale your logic to high-performance and GPU-enabled computing instances in a
single line...

```
@large_gpu_task() # 4 GPU, 32 CPU, 256 RAM
def train_protein_model(samples: LatchDirectory) -> LatchFile:
  ...
```

Peruse the rest of the documentation to learn more.


```{toctree}
:hidden:
:maxdepth: 2
self
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Tutorials
workflow_overview
workflow_io
workflow_metadata
parameter_metadata
task_dependencies
task_overview
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Biocomputing Recipes
mageck
nf-core-rnaseq
alphafold
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: API Docs
subcommands
api/modules
```
