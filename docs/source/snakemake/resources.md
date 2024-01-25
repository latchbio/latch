# Resources

## CPU

By default, all Snakemake jobs will run on a machine with 4 cpus available.

To modify the number of cpus allocated to the job, modify the `resources` directive of the Snakefile rule as follows:

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

By default, all Snakemake jobs will run on a machine with 8 GB of RAM.

To modify the amount of memory allocated to the job, modify the `resources` directive of the Snakefile rule. For example, to allocate 32 GB of RAM to a task:

```python
rule <rule_name>:
    ...
    resources:
        mem_mb=34360
    ...
```
