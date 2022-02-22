"""Service to initialize boilerplate."""

import os
import textwrap
from pathlib import Path


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

    init_f = pkg_root.joinpath("__init__.py")
    with open(init_f, "w") as f:
        f.write(_gen__init__(pkg_name))

    version_f = pkg_root.joinpath("version")
    with open(version_f, "w") as f:
        f.write("0.0.0")

    docker_f = pkg_root.joinpath("Dockerfile")
    with open(docker_f, "w") as f:
        f.write(_gen_dockerfile())


def _gen__init__(pkg_name: str):

    # TODO: (kenny) format pkg_name s.t. resulting function name is valid with
    # more complete parser

    fmt_pkg_name = pkg_name.replace("-", "_")

    # Within the ASCII range (U+0001..U+007F), the valid characters for identifiers
    # are the same as in Python 2.x: the uppercase and lowercase letters A through Z,
    # the underscore _ and, except for the first character, the digits 0 through 9.
    # https://docs.python.org/3/reference/lexical_analysis.html#grammar-token-identifier

    return textwrap.dedent(
        f'''
                """
                {fmt_pkg_name}
                ~~
                Some biocompute
                """

                from flytekit import task, workflow
                from flytekit.types.file import FlyteFile
                from flytekit.types.directory import FlyteDirectory

                @task()
                def {fmt_pkg_name}_task(
                    sample_input: FlyteFile, output_dir: FlyteDirectory
                ) -> str:
                    return "foo"


                @workflow
                def {fmt_pkg_name}(
                    sample_input: FlyteFile, output_dir: FlyteDirectory
                ) -> str:
                    """Description...

                    {fmt_pkg_name} markdown
                    ----

                    Write some documentation about your workflow in
                    markdown here:

                    > Markdown syntax works as expected.

                    ## Foobar

                    __metadata__:
                        display_name: {fmt_pkg_name}
                        author:
                            name: n/a
                            email:
                            github:
                        repository:
                        license:
                            id: MIT

                    Args:

                        sample_input:
                          A description

                          __metadata__:
                            display_name: Sample Param

                        output_dir:
                          A description

                          __metadata__:
                            display_name: Output Directory
                    """
                    return {fmt_pkg_name}_task(
                        sample_input=sample_input,
                        output_dir=output_dir
                    )
                '''
    )


def _gen_dockerfile():
    return textwrap.dedent(
        """
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
        """
    )
