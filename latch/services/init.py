"""
init
~~~~~
Puts boilerplate project files into users working directory
"""

import os
import textwrap
from pathlib import Path


def init(pkg_name: Path):
    """Puts boilerplate files in the designated path."""

    cwd = Path(os.getcwd()).resolve()
    pkg_root = cwd.joinpath(pkg_name)
    try:
        pkg_root.mkdir(parents=True)
    except FileExistsError:
        raise OSError(
            f"A directory of name {pkg_name} already exists."
            " Remove it or pick another name for your latch workflow."
        )

    pkg_root = pkg_root.joinpath("latch")
    pkg_root.mkdir(parents=True)

    init_f = pkg_root.joinpath("__init__.py")
    with open(init_f, "w") as f:
        f.write(
            textwrap.dedent(
                f'''

                    """
                    {pkg_name}
                    ~~
                    Some biocompute
                    """

                    from flytekit import task, workflow
                    from flytekit.types.file import FlyteFile

                    @task()
                    def {pkg_name}_task(
                        nucleotide: str, fastq_file: FlyteFile, output: FlyteFile
                    ) -> str:
                        return "foo"


                    @workflow
                    def {pkg_name}_wf(nucleotide: str, fastq_file: FlyteFile, output: FlyteFile):
                        """Write a short description about your workflow here.

                        Write a longer descrption about your workflow here.

                        Args:

                            nucleotide:
                              Describe the nucleotide parameter here.

                              __metadata__:
                                display_name: nucleotide sequence
                                rules:
                                  - regex: "^[atcgATCG]+$"
                                    message: Sequence must contain characters a,t,c,g,A,T,G,C

                            fastq_file:
                              Describe the fastq_file parameter here.

                              __metadata__:
                                display_name: fastq_file
                                rules:
                                  - regex: "[^\/]$"
                                    message: Trailing '/' are not allowed in file names
                                  - regex: "(.fastq.gz|.fastq)$"
                                    message: Only .fastq or .fastq.gz extensions are valid


                            output:
                              Describe the output parameter here.

                              __metadata__:
                                display_name: Output
                                output: true

                        """
                        return {pkg_name}_task(
                            nucleotide=nucleotide, fastq_file=fastq_file, output=output
                        )

                    '''
            )
        )

    version_f = pkg_root.joinpath("version")
    with open(version_f, "w") as f:
        f.write("0.0.0")
