# Defining The Workflow Environment

Outside the task and workflow code, environment variables, system packages, and various programs are critical to running specific tasks. For example, a workflow that downloads a binary from a server and uses the aws command line toolkit to get reference data may rely on the apt packages `wget` and `aws-cli` as well as `aws` environment variables.

Under the hood, Latch manages the environment of a workflow using Dockerfiles. A Dockerfile specifies the environment in which the workflow runs. However, Dockerfiles are often overkill for simple applications. To solve having to write a Dockerfile for simple cases, the Latch SDK will automatically generate a Dockefile at registration time for a given workflow directory. The user controls the following files in the workflow directory with the following behavior

## Python PyPI Packages
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

## Local Python Packages
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

## Conda Environment
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

## System (Debian) Packages
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

## R Packages
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

## Environment Variables
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
```
</details>
<br />

To write a Dockerfile, you simply write the commands that you want executed and
specify the files that you want available before your task is run. A Dockerfile
defines your task's "image" (the recipe for its virtual computing environment)
which will become a container that it will execute within at runtime.

---

Here is an example of a Dockerfile used earlier:

```Dockerfile
FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:9a7d-main

# Its easy to build binaries from source that you can later reference as
# subprocesses within your workflow.
RUN curl -L https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.4.4/bowtie2-2.4.4-linux-x86_64.zip/download -o bowtie2-2.4.4.zip &&\
    unzip bowtie2-2.4.4.zip &&\
    mv bowtie2-2.4.4-linux-x86_64 bowtie2

# Or use managed library distributions through the container OS's package
# manager.
RUN apt-get update -y &&\
    apt-get install -y autoconf samtools


# You can use local data to construct your workflow image.  Here we copy a
# pre-indexed reference to a path that our workflow can reference.
COPY data /root/reference
ENV BOWTIE2_INDEXES="reference"

COPY wf /root/wf

# STOP HERE:
# The following lines are needed to ensure your build environement works
# correctly with latch.
ARG tag
ENV FLYTE_INTERNAL_IMAGE $tag
RUN sed -i 's/latch/wf/g' flytekit.config
RUN python3 -m pip install --upgrade latch
WORKDIR /root
```

_Note that we must use the base image specified in the first line to configure
libraries and settings for consistent task behavior._

There are 3 latch-base images available as seen on [github](https://github.com/latchbio/latch-base). 

## Writing Dockerfiles for different tasks

When writing different tasks, defining distinct containers for each task
increases workflow performance by reducing the size of the container scheduled
on Kubernetes. It also helps with code organization by only associating
dependencies with the tasks that need them.

You can write different Dockerfiles and associate them to tasks by passing
their paths to the task definition. When registering a workflow with multiple
containers, note that each container will be rebuilt, which can slow down 
development. 

```
@small_task(dockerfile=Path(__file__).parent.parent / "DockerfileMultiQC")
def sample_task(int: a) -> str:
    return str(a)
```

A full example:

```
# assemble.py

import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile

@small_task(dockerfile=Path(__file__) / "Dockerfile")
def assembly_task(read1: LatchFile, read2: LatchFile) -> LatchFile:

    # A reference to our output.
    sam_file = Path("covid_assembly.sam").resolve()

    _bowtie2_cmd = [
        "bowtie2/bowtie2",
        "--local",
        "-x",
        "wuhan",
        "-1",
        read1.local_path,
        "-2",
        read2.local_path,
        "--very-sensitive-local",
        "-S",
        str(sam_file),
    ]

    subprocess.run(_bowtie2_cmd)

    return LatchFile(str(sam_file), "latch:///covid_assembly.sam")
```

```
# sam_blaster.py

import subprocess
from pathlib import Path

from latch import small_task
from latch.types import LatchFile

# Note the use of paths relative to the location of the __init__.py file

@small_task(dockerfile=Path(__file__) / "Dockerfile")
def sam_blaster(sam: LatchFile) -> LatchFile:

    blasted_sam = Path(sam.name + ".blasted.sam")

    subprocess.run(
        ["samblaster", "-i", sam.local_path, "-o", str(blasted_sam.resolve())]
    )

    return LatchFile(blasted_sam, f"latch:///{blasted_sam.name}")
```

Notice how we can organize task definitions and Docker containers into their
own directories.

```
├── Dockerfile
├── __init__.py
├── assemble
│   ├── Dockerfile
│   └── __init__.py
└── sam_blaster
    ├── Dockerfile
    └── __init__.py
```

## Limitations

The difference between a latch task environment and running your code on your
Linux machine is that we restrict your access to `/dev/` and the networking
stack.  For example, you cannot create mounts using `/dev/fuse` (so mounts are
generally off limits) and you do not have admin access to the networking stack
on the host machine as your task execution does not have root access.

The remote development utilities (`latch develop .`) do not work with
different containers per task and we do not recommend using the two features
together for the time being.
