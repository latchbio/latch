<div align="center">

![biocompute](static/biocompute.png)

# Latch SDK

The Latch SDK is a toolchain to define serverless bioinformatics workflows and
dynamically generate no-code interfaces using python functions.

It is built directly on [Flyte](https://docs.flyte.org) for all the benefits that the Kubernetes-native
workflow orchestration framework provides - task-level type-safety and
containerization, independent task scheduling, and heterogeneous & highly
scalable computing infrastructure.

[Docs](https://docs.latch.bio) • [Installation](#installation) • [Quickstart](#configuration) • [Latch](https://latch.bio)

![side-by-side](static/side-by-side.png)

</div>

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

---

### Examples

We'll maintain a growing list of well documented examples here. Please open a
pull request to feature your own:

  * [Guide Counter](https://github.com/latchbio/wf-guide_counter)
  * [Batch-GE](https://github.com/latchbio/wf-batch_ge)
  * [Seq-to-tree](https://github.com/JLSteenwyk/latch_wf_seq_to_tree)
  * [Codon optimization estimation](https://github.com/JLSteenwyk/latch_wf_codon_optimization)
