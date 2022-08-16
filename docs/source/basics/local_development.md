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

To run a workflow locally, create a `main()` function at the end of the file
and run the workflow with the files and parameters of your choice. 

To run this entrypoint from your own local environment, you can run 
`python -c "from wf import main; main()"`.

Local development is very helpful to iterate your workflow on images you have built.
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


def main():
    bar(a=LatchFile("/root/reads.fa"))
```

Note that all provided values must be python literals and must be valid
parameter values given the respective parameter type.

## File Behavior

When a workflow is executing locally, folders named `mock_latch` and `mock_s3` 
will be mounted into the container, and created if they don't exist yet. These folders
will be local versions of `latch:///` and `s3://` respectively. For example, if you 
want to test a workflow using a LatchFile 'data.txt' as input, you can place the file
in `my_workflow/mock_latch/data.txt` and pass the file to the workflow as `LatchFile('latch:///data.txt')`.
Similarly, if you are returning or creating LatchFiles or LatchDirs in your tasks or workflows,
they will be created in these `mock_latch` and `mock_s3` folders. For example, if your workflow
returns `LatchDir('outputs', 'latch:///outputs')`, the outputs will be created in
and can be accessed in `my_workflow/mock_latch/outputs`.

## Versioning

With local execution, you are using images that you have built locally to test new workflow 
code. This means that the only time you need to build a new image during local-execution is 
when you make changes to that image. Generally this applies if you are changing the dependencies 
downloaded or PATH in the image through the Dockerfile. If you do need to make a change but don't want
to commit changes to the image on the LatchBio platform yet, use the `-u` or `--use-auto-version` 
flag.