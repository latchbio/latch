# Quickstart

In this guide, we will walk through how you can upload a more complex Snakemake workflow to Latch. If you haven't already, we reccommend starting with the [Basic Tutorial](./quickstart.md).

The example being used here comes from []().

## Prerequisites

- Register for an account and log into the [Latch Console](https://console.latch.bio)
- Install a compatible version of Python. The Latch SDK is currently only supported for Python >=3.8 and <=3.11
- Install the [Latch SDK](https://github.com/latchbio/latch#installation) with [snakemake](https://snakemake.readthedocs.io/en/stable/getting_started/installation.html) support. We recommend installing Latch SDK in a fresh environment for best behaviour:

```console
python3 -m venv env
source env/bin/activate
pip install "latch[snakemake]"
```

## Step 1: Clone the Snakemake workflow
