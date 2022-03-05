FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/wf-base:fbe8-main

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
RUN  sed -i 's/latch/wf/g' flytekit.config
RUN python3 -m pip install --upgrade latch
WORKDIR /root
