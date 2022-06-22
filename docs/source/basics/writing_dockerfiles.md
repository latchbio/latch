# Writing Dockerfiles

Dockerfiles allow you to define the computing environment that your task will
execute in.

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

## Limitations

The only limitation between a latch workflow and running your code on your linux
machine is that we restrict your access to `/dev/` and the networking stack.
For example, you cannot create mounts using `/dev/fuse` (so mounts are generally
off limits) and you do not have admin access to the networking stack on the host
machine as your task execution does not have root access.
