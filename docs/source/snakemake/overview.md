# Overview

## Motivation

Latch's Snakemake integration allows developers to build graphical interfaces to expose their Snakemake workflows to wet lab teams. It also provides managed cloud infrastructure for executing the workflow's jobs.

A primary design goal for the Snakemake integration is to allow developers to register existing projects with minimal added boilerplate and modifications to code. Here, we outline these changes and why they are needed.

## Snakemake Workflows on Latch

Recall a Snakemake project consists of a `Snakefile`, which describes workflow
rules in an ["extension"](https://snakemake.readthedocs.io/en/stable/snakefiles/rules.html) of Python and associated Python code imported and called by these rules. To make this project compatible with Latch, we need to do the following:

1. Define [metadata and input file parameters](./metadata.md) for your workflow
2. Build a [container](./environments.md) with all runtime dependencies
3. Ensure your `Snakefile` is compatible with [cloud execution](./cloud.md)
