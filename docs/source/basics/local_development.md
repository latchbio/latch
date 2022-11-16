# Iterative development

Ideas: 
- Sandbox environment for testing and debugging

It is often helpful to iteratively test and debug your workflow before registering them to Latch. The Latch SDK provides the command `latch develop` to enable fast local testing and debugging of workflows on Latch. With `latch develop`, you can start a sandbox environment that is identical to the environment your workflow will be run in.

To open up a session for debugging, first navigate to your workflow directory: 
```
$ ls
covid-wf

$ cd covid-wf
```

Make sure that your workflow is first registered on Latch: 
```
$ latch register --remote .
```

Then, you can open a development session with: 
```
$ latch develop .
```
Output: 
```
Copying your local changes... 
Could not find /Users/hannahle/Documents/GitHub/covid-wf/data - skipping
Could not find /Users/hannahle/Documents/GitHub/covid-wf/scripts - skipping
Done.
Successfully connected to remote instance.
Pulling 4034_covid-wf, this will only take a moment... 
Image successfully pulled.

>>>
```


## Example Iterative Development Flow
To demonstrate how to use `latch develop`, we will walk through an end-to-end flow of building and debugging a workflow from scratch. 

## Prerequisites
* Install [Latch](../getting_started/quick_start.md)
* Have a conceptual understanding of how Latch workflows work through reading the [Quickstart](../getting_started/quick_start.md) and [Authoring your own workflow](../getting_started/authoring_your_workflow.md)

## Building a Simple Variant Calling Workflow 

* Clone the code 



### The difference between `latch develop` and calling a script from Python

### What happens behind when you run `latch develop`


Executing workflows on the LatchBio platform is heavily encouraged for consistent behavior.

Workflows often deal with enormous files that are too large for local development environments and sometimes require computing resources that cannot be accommodated by local machines or are just unavailable (eg. GPUs).  Thus, there are many cases when local executions with smaller files or reduced resources may behave differently than on properly configured cloud infrastructure. Local execution should never be a substitute for testing workflow logic on the platform itself.

However, the ability to quickly iterate and debug task logic locally is
certainly useful for teasing out many types of bugs.

Using a `if __name__ == "__main__":` clause is a useful way to tag local function calls with sample values. Running `python3 wf/__init__.py` will become an entrypoint for quick debugging.

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
