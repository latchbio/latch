# Development and debugging

When developing a workflow, its useful to be able to easily run your task functions so that you can debug issues more
effectively and overall, iterate faster. However, since tasks and workflows need to run in a highly custom, specified
environment (namely, the one defined in your Dockerfile(s)), it isn't always feasible to simply run your workflow
locally.

Say for instance you are writing a workflow that uses Google's [DeepVariant](https://github.com/google/deepvariant)
model - running this locally would be nearly impossible (unless your daily driver is a supercomputer). This means that
debugging this workflow as you are writing it would be difficult and time consuming, as you would have to register it
anew *every time you needed to make an edit*, slowing down the speed of development.

To address this, the Latch SDK comes with a command that allows you to quickly run tasks and debug your environment without having to wait for registration every time. Navigate to a workflow directory you would like to work on and run
 `latch develop .`:

```console
$ latch init test-wf
Downloading workflow data .......
Created a latch workflow called test-wf.
Run
    $ latch register test-wf
To register the workflow with console.latch.bio.

$ cd test-wf
$ latch register --remote .
$ latch develop .
```

This command will drop you into a shell in which you can run your code and inspect your environment.

Output:

```console
$ latch develop .
...
>>>
```

**Note**: The shell environment is built from your Dockerfile each time you register your workflow. If you are debugging a new workflow or making changes to a Dockerfile in an existing workflow and want to run `latch develop`, make sure to register your workflow to Latch with `latch register <path_to_workflow_directory>` beforehand.

## Running tasks

If you want to test a task, create a test file and import your task. Then call the task as a python function using any inputs you would like. For file or directory inputs, the files should be in Latch data.

Below is an example task and the code to test it

```console
cat wf/__init__.py
```

```python
import subprocess

from latch.types import LatchFile

@small_task
def assembly_task(
    read1: LatchFile = LatchFile("latch:///read1.fastq"), # <== these are what the task will be run on
    read2: LatchFile = LatchFile("latch:///read2.fastq"), # <==
) -> LatchFile:

    ...

    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")

...
```

```console
cat scripts/test_task.py
```

```python
from latch.types import LatchFile

import wf

wf.assembly_task(
    read1=LatchFile("latch:///read1.txt"),
    read2=LatchFile("latch:///read2.txt"),
)
```

You can execute the script in `latch develop` like so:

```console
>>> python3 scripts/test_task.py
```

You can execute any python code in your workflow environment. Functions and scripts do not have to be latch specific so you can test library code or binaries as well. Think of the environment as a snapshot of the computer your workflow will get when you execute it.

### Notes on the test environment

It is important that any changes to the code are done on your local machine -- these changes will be synced into the latch develop environment and saved on your local computer as well. Changes made directly in the latch develop environment are not saved and are not synced back to your local computer. Moreover, they may be overwritten in the development process.

We use `rsync` to bring changes from your local workflow directory to the latch develop environment. We recursively copy changes to the `/root` directory in the development environment. For example, this line in the default docker image creates the `wf` directory in the cloud environment:

```Dockerfile
...
COPY wf /root/wf
...
```
Then when running `latch develop`, any changes to files or additional files created in the `wf` directory will be reflected in the development environment.

## Exploring the environment

If you are running into issues with your environment (say, for example, a binary isn't where you expect it to be), you can see for yourself exactly what the environment looks like when running `latch develop`.

---

## Next Steps

Visit the [Learning through An Example](../basics/latch_develop_example.md) page to see an end-to-end flow of how to use `latch develop` to test and debug a simple variant calling pipeline.
