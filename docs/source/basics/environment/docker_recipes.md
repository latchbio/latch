# Common Dockerfile patterns

## Include a Binary

Often times we need a binary in our workflow which we may not want to store in a repository with our workflow code. In this case, we can download a binary in our Dockerfile and unpack it. Below we download and unpack bowtie2.

```Dockerfile
RUN curl -L https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.4.4/bowtie2-2.4.4-linux-x86_64.zip/download -o bowtie2-2.4.4.zip &&\
    unzip bowtie2-2.4.4.zip &&\
    mv bowtie2-2.4.4-linux-x86_64 bowtie2
```

## Run Docker in Docker

Use [`--base-image docker` with `latch init`](../../subcommands.md##latch-init) to use a base workflow environment which includes Docker. An example of running a containerized `bowtie2` aligner in a Latch workflow can be found using `latch init --template docker my_bowtie2_example`.
