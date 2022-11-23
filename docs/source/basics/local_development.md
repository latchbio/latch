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

This command will drop you into a [REPL](https://en.wikipedia.org/wiki/Read%E2%80%93eval%E2%80%93print_loop) in which
you can do several things.

Output:

```console
$ latch develop .

Copying your local changes...
Could not find /Users/hannahle/Documents/GitHub/covid-wf/data - skipping
Could not find /Users/hannahle/Documents/GitHub/covid-wf/scripts - skipping
Done.
Successfully connected to remote instance.
Pulling 4034_covid-wf, this will only take a moment...
Image successfully pulled.

>>>
```

**Note**: The REPL environment contains the dependencies specified in your Dockerfile(s). If you are creating a fresh workflow directory or make changes to your Dockerfile that you want to exist in your development environment, you have to register your workflow to Latch with `latch register <path_to_workflow_directory>`.

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
    read1: LatchFile = LatchFile("latch:///read1.fastq"), # <== these are what the task will be run on
    read2: LatchFile = LatchFile("latch:///read2.fastq"), # <==
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

You can run the task like so:

```console
>>> run assembly_task
```

You can then make changes to the task function. All local changes will be automatically synced to the development session, and you can easily run the new task function for testing.

## Running scripts

If you want to debug multiple tasks at once, you can again do so very easily by using scripts. Simply create a
`scripts` folder inside your workflow directory and write python scripts in there.

```console
mkdir scripts
echo 'print("hello world")' > scripts/hello_world.py
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

Hence using scripts, you can rapidly test the interactions between tasks, increasing our speed of workflow development.

## Exploring the environment

If you are running into issues with your environment (say, for example, a binary isn't where you expect it to be), you can see for yourself exactly what the environment looks like. Simply run `shell` in the REPL to get a fully functional bash session in the Docker container you created.

```console
>>> shell

root@ip-10-0-11-243:~$
```

Now you can explore the environment as you would like, test out programs, and more.

---

## Next Steps

The best way to learn is through examples. Visit the [Learning through An Example](../basics/latch_develop_example.md) page to see an end-to-end flow of how to use `latch develop` to test and debug a simple variant calling pipeline.
