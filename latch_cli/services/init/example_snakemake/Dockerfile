from 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:fe0b-main

RUN curl -L -O \
    https://github.com/conda-forge/miniforge/releases/latest/download/Mambaforge-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Mambaforge-Linux-x86_64.sh -b \
    && rm -f Mambaforge-Linux-x86_64.sh

copy environment.yaml /root/environment.yaml

run /root/mambaforge/bin/mamba env create --name snakemake-tutorial --file environment.yaml
env PATH /root/mambaforge/envs/snakemake-tutorial/bin:$PATH

# Latch SDK
# DO NOT REMOVE
run pip install latch==2.32.6
run mkdir /opt/latch

copy . /root/

copy .latch/snakemake_jit_entrypoint.py /root/snakemake_jit_entrypoint.py

arg tag
env FLYTE_INTERNAL_IMAGE $tag
workdir /root
