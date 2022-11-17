<!-- # Iterative development

Ideas:
- Sandbox environment for testing and debugging

It is often helpful to iteratively test and debug your workflow before registering them to Latch. The Latch SDK provides the command `latch develop` to enable fast local testing and debugging of workflows on Latch.

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

This is false - ayush
`
When a workflow is executing locally, remote path handling will be ignored. This
means there will be no attempt to copy data from remote paths when ingesting or
returning parameter values. Workflow logic will strictly read and write from the
`local_path` property of the `LatchFile`/`LatchDir` type (first argument). -->

# Development and debugging

When developing a workflow, its useful to be able to easily run your task functions so that you can debug issues more
effectively and overall, iterate faster. However, since tasks and workflows need to run in a highly custom, specified
environment (namely, the one defined in your Dockerfile(s)), it isn't always feasible to simply run your workflow
locally.

Say for instance you are writing a workflow that uses Google's [DeepVariant](https://github.com/google/deepvariant)
model - running this locally would be nearly impossible (unless your daily driver is a supercomputer). This means that
debugging this workflow as you are writing it would be difficult and time consuming, as you would have to register it
anew *every time you needed to make an edit*.

To address this, the Latch SDK comes with a command that allows you to quickly run tasks and debug your environment
without having to wait for registration every time. Navigate to a workflow directory you would like to work on and run
 `latch develop .`:

```console
ayush@mbp:~$ latch init test-wf
Downloading workflow data .......
Created a latch workflow called test-wf.
Run
    $ latch register test-wf
To register the workflow with console.latch.bio.
ayush@mbp:~$ cd test-wf
ayush@mbp:~/test-wf$ latch develop .
```

This command will drop you into a [REPL](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) in which
you can do several things.

## Running tasks

If you want to run a task quickly, simply type `run <task_name>` into the prompt. This will run the specified task in a
container with the correct environment and stream all output to your terminal, allowing for quick debugging at the task
level. Since this is running in the Docker container you created with your Dockerfile, you don't have to worry about not
having any of the binaries or the dependencies that you need.

You can configure which inputs the task runs on by changing the default values of the arguments in the task definition.
For example, for the below task definition (taken from the boilerplate generated from `latch init`), running
`run assembly_task` would result in `assembly_task` being run on `LatchFile("latch:///read1.txt")` and
`LatchFile("latch:///read2.txt")`.

```python
@small_task
def assembly_task(
    read1: LatchFile = LatchFile("latch:///read1.txt"), # <== these are what the task will be run on
    read2: LatchFile = LatchFile("latch:///read2.txt"), # <==
) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    _bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "-x",
        "wuhan",
        "-1",
        read1.local_path,
        "-2",
        read2.local_path,
        "--very-sensitive-local",
        "-S",
        str(sam_file),
    ]

    subprocess.run(_bowtie2_cmd)

    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")
```

You can then make changes to the task function and run the new function without any hassle by repeating the above
command.

## Running scripts

If you want to debug multiple tasks at once, you can again do so very easily by using scripts. Simply create a
`scripts` folder and write python scripts in there.

```console
ayush@mbp:~/test-wf$ mkdir scripts
ayush@mbp:~/test-wf$ echo 'print("hello world")' > scripts/hello_world.py
```

You can run any script in the `scripts` folder by using the command `run-script` in the local develop REPL.

```console
>>> run-script scripts/hello_world.py
hello world
```

To use your tasks, simply import them from the `wf` module. For example, the following script runs the same task that
was defined above.

```python
# filename: test.py
#
# Run this as below
#
# >>> run-script scripts/test.py

from latch.types import LatchFile

import wf

wf.assembly_task(
    read1=LatchFile("latch:///read1.txt"),
    read2=LatchFile("latch:///read2.txt"),
)
```

Running this script will run `assembly_task`, just the same as the previous `run` command. You can add to this though,
and run multiple tasks in the same script, as below.

```python
# filename: test.py

from latch.types import LatchFile

import wf

sam = wf.assembly_task(
    read1=LatchFile("latch:///read1.txt"),
    read2=LatchFile("latch:///read2.txt"),
)

wf.sort_bam_task(sam=sam)
```

Hence using scripts, you can rapidly test the interactions between tasks, making workflow development faster and less
frustrating.

## Exploring the environment

If you are running into issues with your environment (say, for example, a binary isn't where you expect it to be), you can see for yourself exactly what the environment looks like. Simply run `shell` in the REPL to get a fully functional bash session in the Docker container you created.

```console
>>> shell

root@ip-10-0-11-243:~$
```

Now you can explore the environment as you would like, test out programs, and more.
