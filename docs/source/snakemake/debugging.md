# Debugging Snakemake

## Local Development

When debugging a Snakemake workflow, it's helpful to run the JIT step locally instead of re-registering your workflow everytime you want to test a change. To address this, the Latch SDK supports local development for Snakemake workflows.

If you are not familiar with the `latch develop` command, please read about [Local Development](../basics/local_development.md) before continuing.

---

Run the following command to use `latch develop` with your Snakemake workflow:

```console
$ latch develop . --snakemake
```

To run the JIT task, create a test file that calls the task function defined in `snakemake_jit_entrypoint.py` with test inputs.

For example:

```console
$ cat scripts/dry_run.py
```

```python
from snakemake_jit_entrypoint import your_workflow_name_jit_register_task

your_workflow_name_jit_register_task(
    read1="latch:///read1.txt",
    read2="latch:///read2.txt",
)

```

You can execute the script in your `latch develop` session like so:

```console
$ python3 scripts/dry_run.py
```

**Note**: If you are using `conda`, your shell may activate the conda base environment by default. To ensure that you are running in the exact same envioronment as the JIT task, either run `conda deactivate` once you enter the shell or disable conda's environment auto activation in your Dockerfile: `RUN conda config --set auto_activate_base false`
