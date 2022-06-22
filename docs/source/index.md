# What is the Latch SDK?

It takes months to build infrastructure with the compute, storage, and user-friendly interface necessary to run bioinformatics pipelines at scale.

The Latch SDK is an open-source toolchain to define serverless bioinformatics workflows with plain python and deploy associated no-code interfaces using single command.

Bioinformatics workflows developed with the SDK automatically receive:

* Instant no-code interfaces for accessibility and publication
* First class static typing
* Containerization and versioning of every registered change
* Reliable and scalable managed cloud infrastructure
* Singe line definition of arbitrary resource requirements (eg. CPU, GPU) for serverless execution

![SDK Overview](./assets/sdk-intro.png)

## Problems Latch SDK solves

**Building the infrastructure to share bioinformatics pipelines at scale is time-consuming.** Bioinformatics is dominated by terabytes of data and workflows that require multiple CPUs or GPUs, making sharing and scaling pipelines difficult. It often take engineering teams 6-12 months to build a robust cloud infrastructure necessary to support the ingestion and execution of bioinformatics pipelines.

Lacth SDK allows developers to upload workflows to the full-featured [Latch Platform](https://console.latch.bio) with ease. The platform is built with Kubernetes, ensuring containerization, portability, and scalability are available out-of-the-box. Behind the scene, Latch takes advantage of AWS spot instances, offering ultra-fast runtimes with extremely low cloud costs for teams.

**Bioinformaticians need to create an intuitive user interfaces for biologists.** As a lab or R&D team grows, the number of biologists per bioinformatician increases to a point where it's no longer sustainable for the bioinformatician to manage requests and run pipelines manually from her computer. Automating pipeline runs returns valuable time to the bioinformatician to focus on writing analyses to generate scientific insights.

With Latch SDK, developers can write the description to their workflow and customize input parameters using plain Markdown. Latch automatically parses the written text and Python function headers to compile a type-safe UI.

**Specifying arbitrary cloud compute and storage resources for bioinformatics pipelines is difficult.** With Latch SDK, there are several Python task decorators that easily allow you to define the resources available at runtime. The framework starts at 2 CPUs and 4 GBs of memory and goes all the way to 31 CPUs, 120 GBs of memory and 1 GPU (24 GBs of VRAM, 9,216 CUDA cores) to easily handle all processing needs.

**Bioinformatics tools face the challenges of irreproducibility.** The lack of proper versioning and dependencies management results in a long tail of poorly documented and unusable bioinformatics software tools.

Latch SDK containerizes and versions the code in the background each time a workflow is registered to the Latch platform. Container images are constructed by parsing user-defined [Dockerfiles](https://docs.docker.com/engine/reference/builder/). Similarly, versions are user-specified as any unique plaintext string. This behavior is a strict requirement of the toolchain and gives us remarkable guarantees with respect to code reproducibility, portability and scalability.

## Problems that the Latch SDK does not yet solve

* **Workflows chaining**: We'll aim to support easy installation and reuse of other Latch SDK workflows in your own workflow.
* **Workflows monitoring**: For batched runs of workflows, we'll aim to provide better dashboard, logs, traces, metrics, and alerting for observability.

## What the Latch SDK is not

* **A pure workflow orchestration engine**: There are many popular workflow orchestration engines, such as Nextflow or Snakemake, that can be run locally from a bioinformatician's machine. Although workflow orchestration is a feature of Latch SDK, Latch also provides managed infrastructure and no-code interface generation. You can also easily bring existing workflow script of any language to Latch (See examples [here](./examples/workflows_examples.md)).
* **A self-hosted solution**: Currently, you cannot write your workflow using Latch SDK and host it in your own AWS instance or an HPC. The infrastructure serving bioinformatics pipelines is fully managed by Latch. This allows us to rapidly iterate to bring on high quality features, give cost and performance guarantees, and ensure that security is offered out-of-the-box.

## Examples

Visit [Examples](../examples/workflows_examples.md) to see real-world bioinformatics workflows being built using the Latch SDK

---

## Next Steps

To get started with Latch SDK, view the following resources:

* **[Quickstart](./getting_started/quick_start.md)** is the fastest way to get started with the Latch SDK.
* **[Concepts](./basics/what_is_a_workflow.md)** describes all important Latch SDK concepts.
* **[Examples](./examples/workflows_examples.md)** show full examples of using Latch SDK for various bioinformatics pipelines.
* **[Troubleshooting](./troubleshooting/troubleshooting)** provides a guide to debug common errors.
* **[Reference](./api/modules.rst)** contains detailed API and design documents.
* **[Subcommands](./subcommands.md)** contains details about the Latch command line toolchain to register workflows and upload data to Latch.
* Join the <a href="https://forms.gle/sCjr8tdjzx5HjVW27" target="_blank">SDK open-source community</a> on Slack!

---

```{toctree}
:hidden:
:maxdepth: 2
self
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Getting Started
getting_started/quick_start
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Concepts
basics/what_is_a_workflow
basics/parameter_types
basics/working_with_files
basics/customizing_interface
basics/defining_cloud_resources
basics/writing_dockerfiles
basics/local_development
basics/conditional_and_map_tasks
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Examples
examples/workflows_examples
examples/docker_recipes
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: API Docs
subcommands
api/modules
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Troubleshooting
troubleshooting/troubleshooting.md
```
