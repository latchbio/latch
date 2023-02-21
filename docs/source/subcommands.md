# CLI Reference

## `latch init`

```console
$ latch init PACKAGE_NAME
```

This command is used to initialize a new workflow root by populating it with the required boilerplate. Optionally, a template workflow can be used as a starting point.

`PACKAGE_NAME` is the name of the target directory. It will be created if it does not exist.

### Options

#### `--template`, `-t`

One of `r`, `conda`, `subprocess`, `empty`. If not provided, user will be prompted for input. The `r` template comes with `R4.0` preinstalled, the `conda` template comes with `miniconda` preinstalled + a boilerplate `environment.yaml` conda environment file, the `subprocess` template demonstrates how to run a subprocess in a task, and the `empty` template is a blank slate.

#### `--dockerfile`, `-d`

Generate a Dockerfile for the workflow instead of relying on [auto-generation.](basics/defining_environment.md#automatic-dockerfile-generation)

#### `--base-image`, `-b`

Each environment is build on one of the following base distributions:
- `default` with no additional dependencies
- `cuda` with Nvidia CUDA/cuDNN (cuda 11.4.2, cudnn 8) drivers
- `opencl` with OpenCL (ubuntu 18.04) drivers
- `docker` with the Docker daemon

Only one option can be given at a time. If not provided or `default`, only the bare minimum packages to execute the workflow will be installed.

## `latch register`

This command turns a workflow directory into a cloud native workflow executable from Latch Console. See [writing a workflow](basics/what_is_a_workflow.md) to get ready for this step.

```console
$ latch register PATH_TO_WORKFLOW_DIRECTORY
```

The first argument specifies the local path in which to look for workflow objects. Inside the local path should be a single directory called `wf` containing `__init__.py`, any helper python files, a `Dockerfile`, and a `version` files. See [here](basics/what_is_a_workflow.md) for more info.

### Options

#### `--disable-auto-version`, `-d`

Do not include the workflow contents hash in the workflow version. The `version` must be manually updated after each registration. This can be useful when publishing a workflow with a version that should not include the hash.

#### `--remote`, `-r`

Use a remote server to build the workflow.

## `latch dockerfile`

Generate a `Dockerfile` using files in the specified workflow root.

```console
$ latch dockerfile PATH_TO_WORKFLOW_DIRECTORY
```

## `latch get-params`

```console
$ latch get-params WORKFLOW_NAME
```

This command will generate a dictionary of python-native parameters from the
workflow `WORKFLOW_NAME`, which can then be passed to `latch launch` (documented below). For example, running

```console
$ latch get-params latch.alphafold_wf
```

will generate a param file called `latch.alphafold_wf.params.py` whose contents are as below:

```python
"""Run `latch launch latch.alphafold_wf.params.py` to launch this workflow"""

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

## `latch launch`

```console
$ latch launch [--version=VERSION] PARAM_FILE
```

This command allows a user to launch the workflow and parameters described in `PARAM_FILE`. If `--version` is provided, then that particular version will be executed. If it isn't provided, then it will default to the latest version. See `latch get-params` for more info on parameter files.

## `latch get-wf`

```console
$ latch get-wf [--name=NAME]
```

This command will list out all workflows (and their respective versions) that the user is currently subscribed to. If `--name` is provided, the command will instead list all available versions of the workflow with the given name.

### Options

#### `--name`

If provided, list only the versions with the specified name.

## `latch open`

```console
$ latch open REMOTE_FILE
```

This allows a user to view any of their files in Latch Console using a single command - for example, `$ latch open welcome/welcome.pdf` will open welcome.pdf in Latch Console in the user's browser. Again, the prefix `latch:///` is optional. Note that the path specified must be valid and must point to a file, i.e. this command will throw an error if the argument is a remote directory.

## `latch cp`

```console
$ latch cp SOURCE_PATH DESTINATION_PATH
```

This is the main command-line utility to facilitate data transfer between local machines and Latch. Using `latch cp`, you can copy local files/folders into your Latch file-system and vice versa. Either the `source-path` or the `destination-path` must be an absolute remote path prefixed by `latch:///`. If `source-path` is remote, then the command will download that remote entity to your local file system and save it to `destination-path`. On the other hand, if `destination-path` is remote, then the command will upload the local entity at `source-path` to Latch and save it to `destination-path`.

Here are some examples:

```console
$ latch cp sample.fa latch:///sample.fa
```

This will create a new file visible in Latch Console called sample.fa, located in the root of the user's Latch filesystem

```console
$ latch cp sample.fa latch:///dir1/dir2/sample.fa # where /dir1/dir2 is a valid directory
```

This will create a new file visible in Latch Console called sample.fa, located in the nested directory /dir1/dir2/

```console
$ latch cp latch:///sample.fa sample.fa
```

This will create a new file in the user's local working directory called sample.fa, which has the same contents as the remote file.

```console
$ latch cp latch:///dir1/dir2/sample.fa /dir3/dir4/sample.fa
```

This will create a new file in the local directory /dir3/dir4/ called sample.fa, which has the same contents as the remote file.

## `latch ls`

```console
$ latch ls [REMOTE_DIRECTORY_PATH]
```

Similar to the `ls` command in Unix, the `latch ls` command lists the files and subdirectories within a specified remote directory, defaulting to the user's root if no arguments are provided. The prefix `latch:///` for signifying remote paths is optional, as all paths provided to `latch ls` are assumed remote.

Here are some examples:

```console
$ latch ls # lists all files under latch:/// (i.e. root directory)
$ latch ls welcome # lists all files under latch:///welcome/
$ latch ls latch:///welcome/casTLE # lists all files under latch:///welcome/casTLE
```

## `latch test-data`

A set of subcommands to manipulate managed test data.

### `latch test-data ls`

```console
$ latch test-data ls
```

List test data objects as full S3 paths.

### `latch test-data remove`

```console
$ latch test-data remove S3_PATH
```

Remove a test data object by passing an S3 path.

### `latch test-data upload`

```console
$ latch test-data upload LOCAL_PATH
```

Upload an object to a managed S3 bucket by passing a local path.

## `latch exec`

```console
$ latch exec TASK_NAME
```

Drops the user into an interactive shell from within a task. See [here](basics/remote_execution.md)
for more info.

## `latch preview`

```console
$ latch preview WORKFLOW_NAME
```

Creates a preview of your workflow interface without re-registration.

## `latch workspace`

```console
$ latch workspace
```

Spawns an interactive terminal prompt allowing users to choose what workspace
they want to work in. Allows users to choose between, eg. personal and team
workspaces, to upload files or register workflows.

## `latch get-executions`

```console
$ latch get-executions
```

Spawns a terminal user interface which mimics the listing of executions found in
the browser. Browse through your previous executions, view logs, and manage your
running executions all without leaving your terminal.

## `latch develop`

```console
$ latch develop PATH_TO_WORKFLOW_DIRECTORY
```

Creates a REPL that allows for quick iteration and debugging during workflow
development. See [here](basics/local_development.md) for more info.
