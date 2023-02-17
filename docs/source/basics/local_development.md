# Development and debugging

When developing a workflow, it's helpful to run the task functions before executing the entire workflow in the cloud to debug the environment and logical issues. Since tasks run in a different environment than your local computer (namely, the one defined in your Dockerfile(s)), there may be discrepancies when running your code locally.

To address this, the Latch SDK comes with a command that allows you to run tasks and debug your environment without having to reregister your workflow and run it through the UI.

## Setup

Navigate to a workflow directory you would like to work on and run

```console
$ cd test-wf
$ latch register --remote .
$ latch develop .
```

This sequence of commands will drop you into a shell where you can run your code and inspect the environment.

Output:

```console
$ latch develop .
...Successfully connected to remote instance.
Setting up local sync...
Done.
Pulling 812206152185.dkr.ecr.us-west-2.amazonaws.com/...
Image successfully pulled.
Starting OpenBSD Secure Shell server: sshd.
>>>
```

**Note**: The shell environment is built from the workflow Dockerfile each time you register your workflow. If you are debugging a new workflow or making changes to a Dockerfile in an existing workflow and want to run `latch develop`, make sure to register your workflow to Latch with `latch register <path_to_workflow_directory>` beforehand.

## Running tasks

To test a task, create a test file and import the task. Then call the task using any input you would like. For file or directory inputs, the files should be in Latch data.

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

You can execute any python code in the develop environment. Functions and scripts do not have to be latch SDK code -- you can also test library code or binaries. Think of the environment as a snapshot of the computer your tasks run on.

## Notes on the test environment
Any changes to the code must happen on your local machine -- these changes will be synced into the latch development environment and saved on your local computer as well. Changes made directly in the latch develop environment are not saved and are not synced back to your local computer, and moreover, they may be overwritten by the sync process.

We use `rsync` to bring changes from your local workflow directory to the latch develop environment. We recursively copy changes to the `/root` directory in the development environment. For example, this line in the default docker image creates the `wf` directory in the cloud environment:

```Dockerfile
...
copy . /root/
...
```
Then when running `latch develop`, any changes to files or additional files created in the `wf` directory will be reflected in the environment, overwriting the old code in the development environment to ensure that your latest changes are present.

Files deleted locally are not automatically deleted in the development environment. Also, any changes to the Dockerfile that you would like to reflect in the development environment require a reregister.

## Next Steps

Visit the [learning through An Example](../basics/latch_develop_example.md) page to see an end-to-end flow of how to use `latch develop` to test and debug a simple variant calling pipeline.
