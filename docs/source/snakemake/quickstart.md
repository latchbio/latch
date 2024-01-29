# Quickstart

## Motivation

Latch's Snakemake integration allows developers to build graphical interfaces to expose their Snakemake workflows to wet lab teams. It also provides managed cloud infrastructure for executing the workflow's jobs.

A primary design goal for the Snakemake integration is to allow developers to register existing projects with minimal added boilerplate and modifications to code.

## Uploading an Existing Workflow to Latch

The following guide will outline how to upload an existing Snakemake workflow to the Latch console with three simple commands. If you do not already have a Snakemake workflow that you would like to register, see the [Tutorial](./tutorial.md) to get started.

### Prerequisites

- Register for an account and log into the [Latch Console](https://console.latch.bio)
- Install a compatible version of Python. The Latch SDK is currently only supported for Python >=3.8 and <=3.11
- Install the [Latch SDK](https://github.com/latchbio/latch#installation) with [Snakemake](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html) support. We recommend installing Latch SDK in a fresh environment for best behavior:

```console
$ python3 -m venv env
$ source env/bin/activate
$ pip install "latch[snakemake]"
```

- Verify the Latch SDK version >= 2.38.6

```console
$ latch --version
latch, version 2.38.6
```

### Step 1: Generate Metadata

Every Latch workflow requires the developer to define a workflow metadata object. The Latch Console uses this metadata to expose workflow input parameters to scientists in the UI. The Latch SDK provides a command to automatically generate this metadata file from a `config.yaml`.

```console
latch generate-metadata <path to config file>
```

Be sure to inspect the generated files in the `latch_metadata` folder to verify that the types of the input parameters are as expected. Note that all input files hosted on Latch Data must be either a `LatchFile` or `LatchDir` type.

To learn more about Latch metadata for Snakemake, click [here](./metadata.md).

### Step 2: Define Container Environment

All Snakemake workflows run in a Docker container, which includes the Latch-specific dependencies required to run workflows on the Latch platform. To generate this Dockerfile, run the following command in your root directory:

```console
latch dockerfile . --snakemake
```

By default, each Snakemake job will execute in this Docker container; therefore, this Dockerfile should specify runtime dependencies for your workflow. If your workflow has an `environment.yaml` in the root directory, the generated Dockerfile will use conda to install the packages in your environment file. Otherwise, you will need to install dependencies manually.

For more advanced environment setups (such as the use of the `container` and `conda` Snakemake directives), click [here](./environments.md).

### Step 3: Register the Workflow

To register a Snakemake workflow to Latch, type:

```console
latch register . --snakefile Snakefile
```

We highly recommend reading about the [Snakemake Execution Lifecycle](./lifecycle.md) on Latch to understand what happens after registering and executing your workflow.

### Next Steps

- Ensure your `Snakefile` is compatible with [cloud execution](./cloud.md) on Latch.
- See the [Resources](./resources.md) guide to configure resource requirements for your workflow.
- See the [Troubleshooting](./troubleshooting.md) guide for debugging common workflow issues.
