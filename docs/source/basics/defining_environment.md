# The Workflow Environment

Outside of task code, various environment variables, system packages, and programs are critical to running latch workflows. For example, a task that downloads a binary from a server and uses the aws command line toolkit to get reference data may rely on the apt packages `wget` and `aws-cli` as well as `aws` specific environment variables.

Latch manages the execution environment of a workflow using Dockerfiles. A Dockerfile specifies the environment in which the workflow runs.

Dockerfiles are often overkill for simple applications. To solve having to write a Dockerfile for simple cases, the Latch SDK will automatically generate a Dockefile at registration time for a given workflow directory.

## Environment Definition Files

The workflow author controls optional files in the workflow directory that are used to generate the Dockerfile if present. Below is an exhaustive list of the files used to auto-generate a Dockerfile. If this list does not cover a use case, please open an issue on the [Latch SDK Github](https://github.com/latchbio/latch), and we will respond shortly.

<br>

### Python PyPI Packages
Python requirements found in `requirements.txt` are installed into the default python that executes task code.

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
<summary>Docker commands</summary>

```Dockerfile
copy requirements.txt /opt/latch/requirements.txt
run pip install --requirement /opt/latch/requirements.txt
```
</details>
<br />

### Local Python Packages
Local python packages, indicated by a `setup.py` or `pyproject.toml` file in the workflow directory, will be installed into the default python that executes task code.

<details>
<summary>Example File</summary>

```python
from setuptools import find_packages, setup

setup(
    name="latch",
    version="v2.12.1",
    author_email="kenny@latch.bio",
    description="The Latchbio SDK",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.8",
    entry_points={
        "console_scripts": [
            "latch=latch_cli.main:main",
        ]
    },
    install_requires=[
        "awscli==1.25.22",
    ],
    classifiers=[
        "Programming Language :: Python :: 3.10",
    ],
)
```
</details>
<br />

<details>
<summary>Docker commands</summary>

```Dockerfile
run pip install --editable /root/
```
</details>
<br />

### Conda Environment
A conda python environment, indicated by a `environment.yml` or an `environment.yaml` file, will be used to create an isolated python environment in which all shell commands execute. Any subprocess run with `shell=True` will have access to the binaries and packages defined in the environment file. Miniconda is installed into the workflow environment as the conda manager.

<details>
<summary>Example File</summary>

```yaml
name: env-name
channels:
  - conda-forge
  - defaults
dependencies:
  - python=3.7
  - codecov
variables:
  VAR1: valueA
  VAR2: valueB
```
</details>
<br />

<details>
<summary>Docker commands</summary>

```Dockerfile
env CONDA_DIR /opt/conda
env PATH=$CONDA_DIR/bin:$PATH
run apt-get update --yes && \
    apt-get install --yes curl && \
    curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
    mkdir /root/.conda && \
    # docs for -b and -p flags: https://docs.anaconda.com/anaconda/install/silent-mode/#linux-macos
    bash Miniconda3-latest-Linux-x86_64.sh -b -p /opt/conda && \
    rm -f Miniconda3-latest-Linux-x86_64.sh && \
    conda init bash
copy environment.yml /opt/latch/environment.yml
run conda env create --file /opt/latch/environment.yml --name workflow
shell ["conda", "run", "--name", "workflow", "/bin/bash", "-c"]
run pip install --upgrade latch
```
</details>
<br />

### R Packages
A local R environment will be created using the `environment.R` script. This script should include all the R installs needed to run relevant R scripts in the workflow. R 4.0 is the default version.

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
<summary>Docker commands</summary>

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

### System (Debian) Packages
Requirements found in `system-requirements.txt` are installed system-wide using apt.

<details>
<summary>Example File</summary>

```
autoconf
samtools
```
</details>
<br />


<details>
<summary>Docker commands</summary>

```Dockerfile
copy system-requirements.txt /opt/latch/system-requirements.txt
run apt-get update --yes && xargs apt-get install --yes </opt/latch/system-requirements.txt
```
</details>
<br />

### Environment Variables
Environment variables found in the `environment` file will be used to set environment variables inside your workflow environment.

<details>
<summary>Example File</summary>

```
BOWTIE2_INDEXES=reference
PATH="$PATH:/root/bowtie2"
```
</details>
<br />

<details>
<summary>Docker commands</summary>

```Dockerfile
env {line1}
env {line2}
...
```
</details>
<br />

---
<br>

To save the generated Dockerfile in the workflow directory, run `latch dockerfile {path_to_workflow_directory}`. The generated Dockefile will be used during subsequent `latch register` and `latch develop` commands to build the workflow environment, and it can be user modified. To go back to the autogenerated Dockerfile, delete the Dockerfile in the workflow directory.

To understand the generated Dockefile, here is an example with instructive comments generated by running `latch init --template subprocess --dockerfile pkg_name`:

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

Latch has three base images, one baseline, one with CUDA drivers, and one with OPENCL drivers. To use the CUDA or OPENCL base image, modify the from directive in the Dockerfile to `.../`latch-base-cuda`:...` or `.../`latch-base-opencl`:...`.

## Ignoring files
In the above Dockerfile, the line 

```Dockerfile
copy . /root/
```

recursively copies files and directories from the workflow directory into the Docker image. Even if files in the workflow directory are unused by the workflow, they will be copied into the docker image. Having unnecessary files in the image increases its size, slowing workflow execution and registration. Additionally, any changes to said files will cause image rebuilding when running `latch register` or `latch develop` on the workflow, making the process slower. To avoid this, Latch uses a `.dockerignore` file in the workflow directory. This file should contain a list of files and directories that should be skipped when copying files into your container, as documented [here](https://docs.docker.com/engine/reference/builder/#dockerignore-file). Each workflow begins with a default `.dockerignore` file that ignores common pesky files.

## Docker Limitations

The difference between a latch task environment and running code on a Linux machine is that we restrict access to root system resources. For example, `/dev` and the networking stack are restricted, so creating mounts using `/dev/fuse` is not permitted. We limit this behavior to prevent users from accessing sensitive system resources that could influence other tasks running on the same machine. In the future, we will support full container isolation, allowing users to treat their containers as complete linux machines.


---

```{toctree}
:hidden:
:maxdepth: 2
environment/dockerfile_per_task
environment/docker_recipes
```
