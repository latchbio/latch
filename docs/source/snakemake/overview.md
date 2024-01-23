# Overview

## Motivation

Latch's snakemake integration allows developers to build graphical interfaces to expose their Snakemake workflows to wet lab teams. It also provides managed cloud infrastructure for the execution of the workflow's jobs.

A primary design goal for the Snakemake integration is to allow developers to register existing projects with minimal added boilerplate and modifications to code. Here, we outline these changes and why they are needed.

## Snakemake Workflow's on Latch

Recall a snakemake project consists of a `Snakefile` , which describes workflow
rules in an ["extension"](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html) of Python, and associated python code imported and called by these rules. To make this project compatible with Latch, we need to do the following:

1. Define [metadata and input file parameters](./metadata.md) for your workflow
2. Build a [container](./environments.md) with all runtime dependencies
3. Ensure your `Snakefile` is compatible with [cloud execution](./cloud.md)

## Next Steps

- Go through one of our [tutorials](./tutorials/quickstart.md).
- Learn more about the lifecycle of a Snakemake workflow on Latch by reading our [manual](../lifecycle.md).
- Learn about how to modify Snakemake workflows to be cloud-compatible [here](../cloud.md).
- Visit the repository of [public examples](https://github.com/latchbio/latch-snakemake-examples) of Snakemake workflows on Latch.
