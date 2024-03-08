# DO NOT CHANGE
from 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:fe0b-main

workdir /tmp/docker-build/work/

shell [ \
    "/usr/bin/env", "bash", \
    "-o", "errexit", \
    "-o", "pipefail", \
    "-o", "nounset", \
    "-o", "verbose", \
    "-o", "errtrace", \
    "-O", "inherit_errexit", \
    "-O", "shift_verbose", \
    "-c" \
]
env TZ='Etc/UTC'
env LANG='en_US.UTF-8'

arg DEBIAN_FRONTEND=noninteractive

run apt-get update && \
    apt-get install -y openjdk-17-jdk-headless curl unzip

# Install Mambaforge
run curl \
        --location \
        --fail \
        --remote-name \
        https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh && \
    bash Mambaforge-Linux-x86_64.sh -b -p /opt/conda -u && \
    rm Mambaforge-Linux-x86_64.sh

# Set conda PATH
env PATH=/opt/conda/bin:$PATH

workdir /root

RUN curl -s https://www.bioinformatics.babraham.ac.uk/projects/fastqc/fastqc_v0.12.1.zip -O &&\
    unzip *.zip &&\
    chmod u+x /root/FastQC/fastqc &&\
    ln -s /root/FastQC/fastqc /usr/bin/fastqc

# Latch SDK
# DO NOT REMOVE

# Copy workflow data (use .dockerignore to skip files)
copy . /root/

run /opt/conda/bin/pip install latch==2.39.0.dev0
run mkdir /opt/latch

# Nextflow funsies
copy .latch/bin/nextflow /root/nextflow
copy .latch/.nextflow /root/.nextflow
copy .latch/nf_entrypoint.py /root/nf_entrypoint.py

# Latch workflow registration metadata
# DO NOT CHANGE
arg tag
# DO NOT CHANGE
env FLYTE_INTERNAL_IMAGE $tag

workdir /root
