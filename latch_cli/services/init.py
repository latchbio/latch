"""Service to initialize boilerplate."""

import os
import textwrap
from pathlib import Path

import boto3
from botocore import UNSIGNED
from botocore.config import Config


def init(pkg_name: Path):
    """Creates boilerplate workflow files in the user's working directory.

    Args:
        pkg_name: A identifier for the workflow - will name the boilerplate
            directory as well as functions within the constructed package.

    Example: ::

        init("foo")

    The resulting file structure will look like::

        foo
        ├── __init__.py
        └── version

    Where `version` holds the workflow's version in plaintext and `__init__.py`
    contains the objects needed to define the workflow.


    """

    cwd = Path(os.getcwd()).resolve()
    pkg_root = cwd.joinpath(pkg_name)
    try:
        pkg_root.mkdir(parents=True)
    except FileExistsError:
        raise OSError(
            f"A directory of name {pkg_name} already exists."
            " Remove it or pick another name for your latch workflow."
        )

    wf_root = pkg_root.joinpath("wf")
    wf_root.mkdir(exist_ok=True)
    init_f = wf_root.joinpath("__init__.py")
    with open(init_f, "w") as f:
        f.write(_gen__init__(pkg_name))

    version_f = pkg_root.joinpath("version")
    with open(version_f, "w") as f:
        f.write("0.0.0")

    docker_f = pkg_root.joinpath("Dockerfile")
    with open(docker_f, "w") as f:
        f.write(_gen_dockerfile())

    data_root = pkg_root.joinpath("data")
    data_root.mkdir(exist_ok=True)

    ref_ids = [
        "wuhan.1.bt2",
        "wuhan.2.bt2",
        "wuhan.3.bt2",
        "wuhan.4.bt2",
        "wuhan.fasta",
        "wuhan.rev.1.bt2",
        "wuhan.rev.2.bt2",
    ]

    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))

    print("Downloading workflow data ", flush=True, end="")
    for id in ref_ids:
        print(".", flush=True, end="")
        with open(data_root.joinpath(id), "wb") as f:
            s3.download_fileobj("latch-public", f"sdk/{id}", f)
    print()


def _gen__init__(pkg_name: str):

    # TODO: (kenny) format pkg_name s.t. resulting function name is valid with
    # more complete parser
    # fmt_pkg_name = pkg_name.replace("-", "_")

    # Within the ASCII range (U+0001..U+007F), the valid characters for identifiers
    # are the same as in Python 2.x: the uppercase and lowercase letters A through Z,
    # the underscore _ and, except for the first character, the digits 0 through 9.
    # https://docs.python.org/3/reference/lexical_analysis.html#grammar-token-identifier

    return textwrap.dedent(
        '''
            """
            Assemble and sort some COVID reads...
            """

            import subprocess
            from pathlib import Path

            from latch import small_task, workflow
            from latch.types import LatchFile


            @small_task
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


            @small_task
            def sort_bam_task(sam: LatchFile) -> LatchFile:

                bam_file = Path("covid_sorted.bam").resolve()

                _samtools_sort_cmd = [
                    "samtools",
                    "sort",
                    "-o",
                    str(bam_file),
                    "-O",
                    "bam",
                    sam.local_path,
                ]

                subprocess.run(_samtools_sort_cmd)

                return LatchFile(str(bam_file), "latch:///covid_sorted.bam")


            @workflow
            def assemble_and_sort(read1: LatchFile, read2: LatchFile) -> LatchFile:
                """Description...

                markdown header
                ----

                Write some documentation about your workflow in
                markdown here:

                > Regular markdown constructs work as expected.

                # Heading

                * content1
                * content2

                __metadata__:
                    display_name: Assemble and Sort FastQ Files
                    author:
                        name:
                        email:
                        github:
                    repository:
                    license:
                        id: MIT

                Args:

                    read1:
                      Paired-end read 1 file to be assembled.

                      __metadata__:
                        display_name: Read1

                    read2:
                      Paired-end read 2 file to be assembled.

                      __metadata__:
                        display_name: Read2
                """
                sam = assembly_task(read1=read1, read2=read2)
                return sort_bam_task(sam=sam)
                '''
    ).lstrip()


def _gen_dockerfile():
    return textwrap.dedent(
        """
        FROM 812206152185.dkr.ecr.us-west-2.amazonaws.com/latch-base:6839-main

        RUN apt-get install -y curl unzip

        # Its easy to build binaries from source that you can later reference as
        # subprocesses within your workflow.
        RUN curl -L https://sourceforge.net/projects/bowtie-bio/files/bowtie2/2.4.4/bowtie2-2.4.4-linux-x86_64.zip/download -o bowtie2-2.4.4.zip &&\\
            unzip bowtie2-2.4.4.zip &&\\
            mv bowtie2-2.4.4-linux-x86_64 bowtie2

        # Or use managed library distributions through the container OS's package
        # manager.
        RUN apt-get update -y &&\\
            apt-get install -y autoconf samtools


        # You can use local data to construct your workflow image.  Here we copy a
        # pre-indexed reference to a path that our workflow can reference.
        COPY data /root/reference
        ENV BOWTIE2_INDEXES="reference"

        # STOP HERE:
        # The following lines are needed to ensure your build environement works
        # correctly with latch.
        COPY wf /root/wf
        ARG tag
        ENV FLYTE_INTERNAL_IMAGE $tag
        RUN python3 -m pip install --upgrade latch
        WORKDIR /root
        """
    ).lstrip()
