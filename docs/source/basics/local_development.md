# Local Development

Executing workflows on the LatchBio platform is heavily encouraged for
consistent behavior.

Workflows often deal with enormous files that are too large for local
development environments and sometimes require computing resources that cannot
be accommodated by local machines or are just unavailable (eg. GPUs).  Thus,
there are many cases when local executions with smaller files or reduced
resources may behave differently than on properly configured cloud
infrastructure. Local execution should never be a substitute for testing
workflow logic on the platform itself.

However, the ability to quickly iterate and debug task logic locally is
certainly useful for teasing out many types of bugs.

Using a `if __name__ == "__main__":` clause is a useful way to tag local
function calls with sample values. Running `python3 wf/__init__.py` will become
an entrypoint for quick debugging.

To run the same entrypoint _within_ the latest registered container, one can run
`latch local-execute <PATH_TO_WORKFLOW_DIR>`. This gives the same confidence in
reproducible behavior one would usually receive post registration but with the
benefits of fast local development. Note that workflow code is
mounted inside the latest container build so that rebuilds are not consistently
made with rapid changes.

More information [here](https://docs.latch.bio/subcommands.html#latch-local-execute).

Here is an example of a minimal `wf/__init__.py` file that demonstrates local
execution:

```python
from pathlib import Path

from latch import small_task, workflow
from latch.types import LatchFile


@small_task
def foo(a: LatchFile) -> LatchFile:

    with open(a) as f:
        print(f.read())

    b = Path("new.txt").resolve()
    with open(b, "w") as f:
        f.write("somenewtext")

    return LatchFile(str(b), "latch:///remote_location/a.txt")


@workflow
def bar(a: LatchFile) -> LatchFile:
    """
    ...
    """
    return foo(a=a)


if __name__ == "__main__":
    bar(a=LatchFile("/root/reads.fa"))
```

Note that all provided values must be python literals and must be valid
parameter values given the respective parameter type.

## File Behavior

When a workflow is executing locally, remote path handling will be ignored. This
means there will be no attempt to copy data from remote paths when ingesting or
returning parameter values. Workflow logic will strictly read and write from the
`local_path` property of the `LatchFile`/`LatchDir` type (first argument).

## Outputs

To save and view outputs during local execution, you have to store files locally and 
then return either a `LatchFile` or `LatchDir` object pointing to that local path. Due to
the way local-execute works under the hood, you have to return this in a specified 
directory. This default directory is `/root/{output_dir}`, where `output_dir` is by default
`'outputs'` and can be changed with the `--output-dir` flag.

For example, if you run 
```bash
latch local-execute . --output-dir my_outputs
```
then in the workflow you will need to write output files and directories to `/root/my_outputs`.
You don't technically need to return a `LatchFile` or `LatchDir` object pointing
to `/root/my_outputs` during local execution to get the files, but your implementation
should be consistent with how you would return a `LatchFile` or `LatchDir` object in the 
LatchBio platform. For example, if you consistently write output files to `/root/my_outputs`, 
then returning `LatchDir("/root/my_outputs", "latch:///my_outputs")` will have identical 
behavior locally and in the LatchBio platform.


## Versioning

With local-execution, you are using images that you have built locally to test new workflow 
code. This means that the only time you need to build a new image during local-execution is 
when you make changes to that image. Generally this only applies if you are changing the
dependencies downloaded or PATH in the image. If you do need to make a change but don't want
to commit changes to the image on the LatchBio platform yet, use the `-u` or `--use-auto-version` 
flag.