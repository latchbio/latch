# What is the Latch SDK?

It takes months to build infrastructure with the compute, storage, and user-friendly interface necessary to run bioinformatics pipelines at scale.

The Latch SDK is an open-source toolchain to define serverless bioinformatics workflows with plain python and deploy associated no-code interfaces using single command.

Bioinformatics workflows developed with the SDK automatically receive:

* Instant no-code interfaces for accessibility and publication
* First class static typing
* Containerization and versioning of every registered change
* Reliable and scalable managed cloud infrastructure
* Single line definition of arbitrary resource requirements (eg. CPU, GPU, Storage) for serverless execution

![SDK Overview](./assets/sdk-intro.png)

---

## Next Steps

You can create a new workflow from scratch using Latch Python SDK or directly upload an existing Nextflow or Snakemake pipeline.

* [Python SDK](https://wiki.latch.bio/docs/getting-started/quick-start): Good for those with ad-hoc scripts which can be chained together into a workflow
* [Snakemake Integration](https://wiki.latch.bio/docs/snakemake/quickstart): Recommended for those with an existing Snakemake pipeline.
* [Nextflow Integration](https://wiki.latch.bio/docs/nextflow/quickstart): Recommended for those with an existing Nextflow pipeline.

---

```{toctree}
:hidden:
:maxdepth: 2
:caption: API Reference
api/modules
```
