# Defining The Workflow Environment

Outside the task code, environment variables, system packages, and various programs are critical to running latch workflows. For example, a task that downloads a binary from a server and uses the aws command line toolkit to get reference data may rely on the apt packages `wget` and `aws-cli` as well as `aws` specific environment variables.

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

## Writing Dockerfiles for different tasks

When writing different tasks, defining distinct containers for each task
increases workflow performance by reducing the size of the container scheduled
on Kubernetes. It also helps with code organization by only associating
dependencies with the tasks that need them.

To use a separate Dockerfile for a task, pass the path of the Dockerfile when defining a task. If the workflow utilizes more than one Dockerfile, registration will take longer given that multiple containers must be built.

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

We can organize task definitions and Dockerfiles in a directory structure as follows:

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

However, the root directory used when building the images is always the workflow directory.

A limitation of using a separate Dockerfile for a task is that `latch develop .` uses the Dockerfile in the workflow directory. In the future, we will support passing a Dockerfile as an argument to `latch develop`.

## Docker Limitations


The difference between a latch task environment and running code on a Linux machine is that we restrict access to root system resources. For example, `/dev` and the networking stack are restricted, so creating mounts using `/dev/fuse` is not permitted. We limit this behavior to prevent users from accessing sensitive system resources that could influence other tasks running on the same machine. In the future, we will support full container isolation, allowing users to treat their containers as complete linux machines.
