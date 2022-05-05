# Latch SDK Documentation

The Latch SDK is a toolchain to define serverless bioinformatics workflows and
dynamically generate no-code interfaces using python functions.

Workflows developed with the SDK feature:

  * Instant no-code interfaces for accessibility and publication
  * First class static typing
  * Containerization + versioning of every registered change
  * Reliable + scalable managed cloud infrastructure
  * Singe line definition of arbitrary resource requirements (eg. CPU, GPU) for serverless execution

### Quickstart

Getting your hands dirty with SDK is the best way to understand how it works.
Run the following three commands in your terminal to register your first
workflow to LatchBio.

**Prerequisite**: ensure that `docker` is present and running on your machine. 
(Install docker [here](https://docs.docker.com/get-docker/) if you don't already
have it installed.)

First, install latch through `pip`.

```
$ python3 -m pip install latch
```

Then, create some boilerplate code for your new workflow.

```
$ latch init testworkflow
```

Finally register the boilerplate code to [LatchBio](latch.bio).

```
$ latch register testworkflow
```

This might take 3-10 minutes depending on your network connection. (Subsequent
registers will complete in seconds by reusing the image layers from this initial
register.) The registration process will:

  * Build a docker image containing your workflow code
  * Serialize your code and register it with your LatchBio account
  * Push your docker image to a managed container registry

When registration has completed, you should be able to navigate
[here](https://console.latch.bio/workflows) and see your new workflow in your
account.

If you are having issues with registration or have general questions, please
file an issue on [github](https://github.com/latchbio/latch).

---

### Installation

The SDK is distributed on pip. Install in a fresh virtual environment for best
behavior. 

[Virtualenvwrapper]() is recommended.

```
python3 -m pip install latch
```

_Note that a local installation of docker is required to register workflows_.


```{toctree}
:hidden:
:maxdepth: 2
self
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Basics
basics/what_is_a_workflow
basics/parameter_types
basics/working_with_files
basics/customizing_interface
basics/defining_cloud_resources
basics/writing_dockerfiles
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: API Docs
subcommands
api/modules
```
