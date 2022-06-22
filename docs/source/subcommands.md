# CLI Reference

## `latch init`

```shell-session
$ latch init PACKAGE_NAME
```

This command can be used to generate an example workflow ([this workflow](https://genomebiology.biomedcentral.com/track/pdf/10.1186/gb-2009-10-3-r25.pdf), to be specific) for reference purposes. It has all of the [required elements](basics/what_is_a_workflow.md) for a workflow, and can also be used as boilerplate - simply replace the logic with your logic, adding more tasks as necessary.

The parameter `PACKAGE_NAME` is the name of the directory that `latch init` will create and populate with example files. The resulting directory structure is as below:

```text
PACKAGE_NAME
├── Dockerfile
├── data
│   ├── wuhan.1.bt2
│   ├── wuhan.2.bt2
│   ├── wuhan.3.bt2
│   ├── wuhan.4.bt2
│   ├── wuhan.fasta
│   ├── wuhan.rev.1.bt2
│   └── wuhan.rev.2.bt2
├── version
└── wf
    └── __init__.py
```

This example workflow is ready for registration (see below).

## `latch register`

This command turns a workflow directory into a cloud native workflow executable from Latch Console. See [writing a workflow](basics/what_is_a_workflow.md) to get ready for this step.

```shell-session
$ latch register PATH_TO_WORKFLOW_DIRECTORY
```

The first argument specifies the local path in which to look for workflow objects. Inside the local path should be a single directory called `wf` containing `__init__.py`, any helper python files, a `Dockerfile`, and a `version` files. See [here](basics/what_is_a_workflow.md) for more info.

## `latch get-params`

```shell-session
$ latch get-params WORKFLOW_NAME
```

This command will generate a dictionary of python-native parameters from the workflow `WORKFLOW_NAME`, which can then be passed to `latch execute` (documented below). For example, running

```shell-session
$ latch get-params latch.alphafold_wf
```

will generate a param file called `latch.alphafold_wf.params.py` whose contents are as below:

```python
"""Run `latch execute latch.alphafold_wf.params.py` to execute this workflow"""

from latch.types import LatchFile
from latch.types import LatchDir

params = {
    "_name": "latch.alphafold_wf", # Don't edit this value.
    "db": "full", # DEFAULT. <class 'str'>
    "fasta_file": LatchFile("latch:///foobar"), # typing.Union[<class 'latch.types.file.LatchFile'>, <class 'str'>]
    "is_prokaryote": False, # DEFAULT. <class 'bool'>
    "max_template_date": "2022-01-01", # DEFAULT. <class 'str'>
    "mode": "monomer_single", # DEFAULT. <class 'str'>
    "output_dir": LatchDir("latch:///foobar"), # <class 'latch.types.directory.LatchDir'>
    "output_name": "run1", # DEFAULT. <class 'str'>
    "weights_download_url": "https://storage.googleapis.com/alphafold/alphafold_params_2021-10-27.tar", # DEFAULT. <class 'str'>
}
```

## `latch execute`

```shell-session
$ latch execute [--version=VERSION] PARAM_FILE
```

This command allows a user to execute the workflow and parameters described in `PARAM_FILE`. If `--version` is provided, then that particular version will be executed. If it isn't provided, then it will default to the latest version. See `latch get-params` for more info on parameter files.

## `latch local-execute`

```shell-session
$ latch local-execute PATH_TO_WORKFLOW_DIRECTORY
```

Execute a workflow within the latest registered container. Run from the
outside the pkg root, eg. `latch local-execute myworkflow` where
`myworkflow` is the directory containing your workflow package.

This is the same as running `$ python3 wf/__init__.py` within the latest
registered container. Workflow code is overlayed on the latest registered
container as a volume available at container runtime, meaning that changes to
python code do not require full Docker rebuilds. This is ideal for rapid local
iteration. Just ensure that code is re-registered (via `latch register x`)
when changes need to be persisted.

As an aside, we assume the workflow file contains a snippet conducive to local
execution such as:

```python
    if __name__ == "__main___":
       my_workflow(a="foo", reads=LatchFile("/users/von/neumann/machine.txt")
```

## `latch get-wf`

```shell-session
$ latch get-wf [--name=NAME]
```

This command will list out all workflows (and their respective versions) that the user is currently subscribed to. If `--name` is provided, the command will instead list all available versions of the workflow with the given name.

### Options

`--name` -- if provided, refines the output to instead list out all available versions of the workflow with the specified name

## `latch open`

```shell-session
$ latch open REMOTE_FILE
```

This allows a user to view any of their files in Latch Console using a single command - for example, `$ latch open welcome/welcome.pdf` will open welcome.pdf in Latch Console in the user's browser. Again, the prefix `latch:///` is optional. Note that the path specified must be valid and must point to a file, i.e. this command will throw an error if the argument is a remote directory.

## `latch cp`

```shell-session
$ latch cp SOURCE_PATH DESTINATION_PATH
```

This is the main command-line utility to facilitate data transfer between local machines and Latch. Using `latch cp`, you can copy local files/folders into your Latch file-system and vice versa. Either the `source-path` or the `destination-path` must be an absolute remote path prefixed by `latch:///`. If `source-path` is remote, then the command will download that remote entity to your local file system and save it to `destination-path`. On the other hand, if `destination-path` is remote, then the command will upload the local entity at `source-path` to Latch and save it to `destination-path`.

Here are some examples:

```shell-session
$ latch cp sample.fa latch:///sample.fa
```

This will create a new file visible in Latch Console called sample.fa, located in the root of the user's Latch filesystem

```shell-session
$ latch cp sample.fa latch:///dir1/dir2/sample.fa # where /dir1/dir2 is a valid directory
```

This will create a new file visible in Latch Console called sample.fa, located in the nested directory /dir1/dir2/

```shell-session
$ latch cp latch:///sample.fa sample.fa
```

This will create a new file in the user's local working directory called sample.fa, which has the same contents as the remote file.

```shell-session
$ latch cp latch:///dir1/dir2/sample.fa /dir3/dir4/sample.fa
```

This will create a new file in the local directory /dir3/dir4/ called sample.fa, which has the same contents as the remote file.

## `latch ls`

```shell-session
$ latch ls [REMOTE_DIRECTORY_PATH]
```

Similar to the `ls` command in Unix, the `latch ls` command lists the files and subdirectories within a specified remote directory, defaulting to the user's root if no arguments are provided. The prefix `latch:///` for signifying remote paths is optional, as all paths provided to `latch ls` are assumed remote.

Here are some examples:

```shell-session
$ latch ls # lists all files under latch:/// (i.e. root directory)
$ latch ls welcome # lists all files under latch:///welcome/
$ latch ls latch:///welcome/casTLE # lists all files under latch:///welcome/casTLE
```

## `latch touch`

```shell-session
$ latch touch REMOTE_FILE_PATH
```

Similar to the `touch` command in Unix, the `latch touch` command creates an empty file at the path specified. The prefix `latch:///` for signifying remote paths is optional, as all paths provided to `latch touch` are assumed remote.

Here are some examples:

```shell-session
$ latch touch a.txt # creates an empty file called a.txt in the user's root directory
$ latch touch welcome/b.txt # creates an empty file called b.txt in latch:///welcome/
$ latch touch latch:///welcome/b.txt # same result as above
```

Note that all parent directories in the specified remote path must already exist. For example, something like `$ latch touch welcome/doesnt_exist/example.txt` will throw an error. If a file already exists at the specified remote path, this command will overwrite its contents thereby making it empty.

## `latch mkdir`

```shell-session
$ latch mkdir REMOTE_DIRECTORY_PATH
```

Similar to the `mkdir` command in Unix, the `latch mkdir` command creates an empty folder at the path specified. The prefix `latch:///` for signifying remote paths is optional, as all paths provided to `latch mkdir` are assumed remote.

Here are some examples:

```shell-session
$ latch mkdir sample # creates an empty folder called sample in the user's root directory
$ latch mkdir welcome/sample # creates an empty file called sample in latch:///welcome/
$ latch mkdir latch:///welcome/sample # same result as above
```

Note that all parent directories in the specified remote path must already exist. For example, something like `$ latch mkdir welcome/doesnt_exist/example_dir` will throw an error. Moreover, if a directory already exists at the specified remote path, this command will create an indexed version of the directory. For example, the sequence of commands

```shell-session
$ latch mkdir welcome/example_dir
$ latch mkdir welcome/example_dir
```

will create two different directories inside of `latch:///welcome`, one called `example_dir` and the other called `example_dir 1`.

## `latch rm`

```shell-session
$ latch rm REMOTE_PATH
```

Similar to the `rm` command in Unix, the `latch rm` command deletes the entity at the path specified (either a file or a folder). In the case of folders, `latch rm` behaves like `rm -r`, deleting both the folder and its contents. The prefix `latch:///` for signifying remote paths is optional, as all paths provided to `latch mkdir` are assumed remote.

Here are some examples (assume every path is valid):

```shell-session
$ latch rm sample # deletes the entity called sample in the user's root directory
$ latch rm welcome/sample # creates an empty file called sample in latch:///welcome/
$ latch rm latch:///welcome/sample # same result as above
```
