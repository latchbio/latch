# Debugging Snakemake

## Local Development

When debugging a Snakemake workflow, it's helpful to run the JIT step locally instead of re-registering your workflow every time you want to test a change. To address this, the Latch SDK supports local development for Snakemake workflows.

If you are unfamiliar with the `latch develop` command, please read about [Local Development](../basics/local_development.md) before continuing.

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

**Note**: If you are running into an `ImportError`, be sure to use the version of Python in which the Latch SDK was installed.
