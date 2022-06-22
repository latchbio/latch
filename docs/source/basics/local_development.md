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
