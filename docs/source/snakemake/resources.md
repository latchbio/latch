# Resources

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
