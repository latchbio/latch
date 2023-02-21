# Workflow Environment

Workflow code is rarely free of dependencies. It may require python or system packages or make use of environment variables. For example, a task that downloads compressed reference data from AWS S3 will need the `aws-cli` and `unzip` [APT](https://en.wikipedia.org/wiki/APT_(software)) packages, then use the `pyyaml` python package to read the included metadata.

The workflow environment is encapsulated in [a Docker container](https://en.wikipedia.org/wiki/Docker_(software)), which is created from a recipe defined in [a text document named Dockerfile.](https://docs.docker.com/engine/reference/builder/). Latch provides [four baseline environments](../subcommands.md#base-image--b) which each latch workflow inherits from. In most cases, modifying the `Dockefile` manually is unnecessary, so Latch will automatically generate one using conventional dependency lists and heuristics. To use a handwritten Dockerfile, [run the eject command](#ejecting-auto-generation).

## Automatic Dockerfile Generation

Below is the list of files used when auto-generating Dockerfiles.

If auto-generation does not cover your use case, please [open a suggestion on GitHub.](https://github.com/latchbio/latch/issues)

### Python: `requirements.txt`

Dependencies from a [`requirements.txt` file](https://pip.pypa.io/en/stable/reference/requirements-file-format/) will be automatically installed using `pip install --requirement`.

<details>
<summary>Example File</summary>

```
boto3==1.20.24
boto3-stubs[s3,sts,sns,ses,logs]
kubernetes
awscli==1.22.24
```
</details>
<br />

<details>
<summary>Generated Docker Commands</summary>

```Dockerfile
copy requirements.txt /opt/latch/requirements.txt
run pip install --requirement /opt/latch/requirements.txt
```
</details>
<br />

### Python: `setup.py`, PEP-621 `pyproject.toml`

Workflows with a package specification in a [`setup.py` file](https://docs.python.org/3/distutils/setupscript.html) or a [PEP-621 `pyproject.toml` file](https://peps.python.org/pep-0621/) will be automatically installed using `pip install --editable`

[Poetry `pyproject.toml` files](https://python-poetry.org/docs/pyproject/) are not supported.

<details>
<summary>Example File</summary>

```python
from setuptools import setup

setup(
    name='alphafold',
    version='2.2.3',
    author='DeepMind',
    ...
)
```
</details>
<br />

<details>
<summary>Generated Dockerfile Commands</summary>

```Dockerfile
copy . /root/
run pip install --editable /root/
```
</details>
<br />

### System/Python: Conda `environment.yaml`

The [Conda](https://docs.conda.io/en/latest/) environment in an [`environment.yaml` file](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually) will be automatically created using `conda env create --file` with latest [miniconda](https://docs.conda.io/en/latest/miniconda.html). The environment will be activated by default.

<details>
<summary>Example File</summary>

```yaml
name: workflow
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.7
  - bwakit=0.7.17
variables:
  reference: ~/covid19
```
</details>
<br />

<details>
<summary>Generated Dockerfile Commands</summary>

```Dockerfile
env CONDA_DIR /opt/conda
env PATH=$CONDA_DIR/bin:$PATH

run apt-get update --yes && \
    apt-get install --yes curl && \
    curl --remote-name https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    mkdir /root/.conda && \
    # docs for -b and -p flags: https://docs.anaconda.com/anaconda/install/silent-mode/#linux-macos
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \
    rm -f Miniconda3-latest-Linux-x86_64.sh && \
    conda init bash

copy environment.yaml /opt/latch/environment.yaml
run conda env create --file /opt/latch/environment.yaml --name workflow

shell ["conda", "run", "--name", "workflow", "/bin/bash", "-c"]
run pip install --upgrade latch
```
</details>
<br />

### R: `environment.R`

Any script in an `environment.R` file will be automatically executed when the workflow is built. This is intended for installing dependencies but there are no actual limits on what the script does.

Currently only R 4.0.0 is supported.

Note that some R packages may have system dependencies that need to be installed using APT or another method. These packages will list these dependencies in their documentation. Missing dependencies will cause crashes during workflow build or when using the packages.

<details>
<summary>Example File</summary>

```R
install.packages("RCurl")
install.packages("BiocManager")

BiocManager::install("S4Vectors")
```
</details>
<br />

<details>
<summary>Generated Dockerfile Commands</summary>

```Dockerfile
run apt-get update --yes && \
    apt-get install --yes software-properties-common && \
    add-apt-repository "deb http://cloud.r-project.org/bin/linux/debian buster-cran40/" && \
    apt-get install --yes r-base r-base-dev libxml2-dev libcurl4-openssl-dev libssl-dev wget

copy environment.R /opt/latch/environment.R
run Rscript /opt/latch/environment.R
```
</details>
<br />

### System: APT

Dependencies from a `system-requirements.txt` text document will be automatically installed using `apt-get install --yes`

<details>
<summary>Example File</summary>

```
autoconf
samtools
```
</details>
<br />


<details>
<summary>Generated Dockerfile Commands</summary>

```Dockerfile
copy system-requirements.txt /opt/latch/system-requirements.txt
run apt-get update --yes && \
    xargs apt-get install --yes < /opt/latch/system-requirements.txt
```
</details>
<br />

### Environment Variables
Environment variables from an `.env` text document will be automatically set in the workflow environment.

<details>
<summary>Example File</summary>

```
BOWTIE2_INDEXES=reference
PATH="/root/bowtie2:$PATH"
```
</details>
<br />

<details>
<summary>Generated Dockerfile Commands</summary>

```Dockerfile
env BOWTIE2_INDEXES="reference"
env PATH="/root/bowtie2:$PATH"
```
</details>
<br />

---
<br>

## Example of Auto-generated Dockerfile

The following Dockerfile is generated in the `subprocess` template (using `latch init --template subprocess --dockerfile example_workflow`):

```Dockerfile
# latch base image + dependencies for latch SDK --- removing these will break the workflow
from 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:ace9-main
run pip install latch==2.12.1
run mkdir /opt/latch

# install system requirements
copy system-requirements.txt /opt/latch/system-requirements.txt
run apt-get update --yes && xargs apt-get install --yes </opt/latch/system-requirements.txt

# copy all code from package (use .dockerignore to skip files)
copy . /root/

# set environment variables
env BOWTIE2_INDEXES=reference

# latch internal tagging system + expected root directory --- changing these lines will break the workflow
arg tag
env FLYTE_INTERNAL_IMAGE $tag
workdir /root
```

## Note on Python Requirements

The order of python requirement installation is as follows
1. `conda`
2. `setup.py` / `pyproject.toml`
3. `requirements.txt`

Consequently, a package specified in the `requirements.txt` file will overwrite a previous install of the same packaged installed by the `conda` environment. 

## Ejecting Auto-generation

The auto-generated Dockerfile can be saved to the workflow root using `latch dockerfile <path to workflow root>`. Subsequent `latch register` and `latch develop` commands will use the saved version. This also disables automatic generation so no dependency files will be used and changes in these files will not have any effect.

To start with a custom Dockerfile, the `--dockerfile` option for `latch init` can be used.

This can be used to switch to a more complicated handwritten Dockerfile or to debug any issues with auto-generation. Removing the Dockerfile will re-enable automatic generation.

If you use ejection because auto-generation does not cover your use case, please [open a suggestion on GitHub.](https://github.com/latchbio/latch/issues)

## Excluding Files

By default, all files in the workflow root directory are included in the workflow build. Any unnecessary files will increase the resulting workflow container image size and increase registration and startup time proportional to their size.

To exclude files from the build use [a `.dockerignore`.](https://docs.docker.com/engine/reference/builder/#dockerignore-file) Files can be specified one at a time or using glob patterns.

The default `.dockerignore` includes files auto-generated by Latch.

## GPU Task Limitations

Commands that require certain [kernel capabilities](https://man7.org/linux/man-pages/man7/capabilities.7.html) will fail with "Permission denied" in GPU tasks (`small-gpu-task`, `large-gpu-task`). This includes `mount` and `chroot` among others.

---

```{toctree}
:hidden:
:maxdepth: 2
environment/dockerfile_per_task
environment/docker_recipes
```
