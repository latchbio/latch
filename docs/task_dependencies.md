## Task Dependencies

Within a task, nearly anything available in Python can be achieved. Often times, users wish to use command line tools, conda packages, or libraries from a task, using the task as a wrapper for the meat of their program. Both these goals are entirely achieveable through the python `subprocess` module. However, there is the question of getting these tools into the task execution environment. To this end, we offer two options. The easier one is to write a `requirements.txt` and pass it into `latch register`. Using this option, the docker image in which your task executes will install these dependencies, allowing you to call any of these packages from within your task. Of course, this assumes that the tools you need to write your task are available in pypi.

In the common case where you need to, say install miniconda and setup a custom conda environment, you can pass in a Dockerfile which creates said execution environment. Said Dockerfile will be chained using the `FROM` command with our Flyte default dockerfile holding our dependencies, and viola, you have everything you need to execute your task. Below is an example of such a Dockerfile in the conda and local packages case:

```
FROM python:3.10.2-buster


# Look to conda first
ENV PATH="/root/miniconda3/bin:${PATH}"
ARG PATH="/root/miniconda3/bin:${PATH}"

# Get miniconda
RUN curl -O \
    https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh \
    && mkdir /root/.conda \
    && bash Miniconda3-latest-Linux-x86_64.sh -b \
    && rm -f Miniconda3-latest-Linux-x86_64.sh

# linux dependencies
RUN apt-get update && apt-get install gcc g++ bowtie2 samtools libsys-hostname-long-perl \
  -y --no-install-recommends \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/* \
  && rm -rf /usr/share/man/* \
  && rm -rf /usr/share/doc/* \
  && conda install -c defaults -c conda-forge -c bioconda -y -n base --debug -c bioconda trimmomatic flash numpy cython jinja2 tbb=2020.2 \
  && conda clean --all --yes


# copy local packages into Dockerfile
COPY ./CRISPResso2 /root/CRISPResso2
WORKDIR /root/CRISPResso2
RUN python setup.py install \
  && CRISPResso -h \
  && CRISPRessoBatch -h \
  && CRISPRessoPooled -h \
  && CRISPRessoWGS -h \
  && CRISPRessoCompare -h
```

Thus using a Dockerfile gives us almost infinite flexibility. Looking at the above commands, we bring both conda and a local python package into our latch execution environment. Then from our task, we have the ability to use conda and our local CRISPResso2 package.

**Note: multiple tasks**: for now, each task in your workflow gets the same environment as defined by the Dockerfile.