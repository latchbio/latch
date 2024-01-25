# Resources

## CPU

By default, all Snakemake jobs will run on a machine with 4 CPUs available. To modify the number of CPUs allocated to the job, use the `resources` directive of the Snakefile rule as follows:

```python
rule <rule_name>:
    ...
    resources:
        cpus=8
    ...
```

Snakemake requires that the user specify the number of cores available to the workflow via the `--cores` command line argument. To define the number of cores available to the job, set the `cores` keyword in your `SnakemakeMetadata`. The `cores` field will default to 4 if there is no value provided.

```python
# latch_metadata/__init__.py
from latch.types.metadata import SnakemakeMetadata, LatchAuthor
from latch.types.directory import LatchDir

from .parameters import generated_parameters

SnakemakeMetadata(
    output_dir=LatchDir("latch:///your_output_directory"),
    display_name="Your Workflow Name",
    author=LatchAuthor(
        name="Your Name",
    ),
    parameters=generated_parameters,
    cores=8, # added
)
```

## Memory

By default, all Snakemake jobs will run on a machine with 8 GB of RAM. To modify the amount of memory allocated to the job, use the `resources` directive of the Snakefile rule. For example, to allocate 32 GB of RAM to a task:

```python
rule <rule_name>:
    ...
    resources:
        mem_mb=34360
    ...
```

## GPU

To run a Snakemake job on a GPU instance, modify the `resources` directive of the Snakefile rule. For example:

```python
rule <rule_name>:
    ...
    resources:
        nvidia_gpu=1
    ...
```

GPU tasks will execute as either a `small_gpu_task` or `large_gpu_task` as defined [here](https://docs.latch.bio/basics/defining_cloud_resources.html#prespecified-task-resource). To request a large GPU instance, add CPU and memory requirements as follows:

```python
rule <rule_name>:
    ...
    resources:
        nvidia_gpu=1
        cpus=8
        mem_mb=33286
    ...
```

Limitations:

1. Using the `container` directive inside GPU instances is currently not supported. Use conda or add runtime dependencies to your Dockerfile to use GPUs.
2. Multi-GPU instances are currently not supported. The JIT workflow will fail if more than 1 GPU is requested.
