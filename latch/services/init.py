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
                    from flytekit.types.directory import FlyteDirectory

                    @task()
                    def {pkg_name}_task(
                        sample_input: FlyteFile, output_dir: FlyteDirectory
                    ) -> str:
                        return "foo"


                    @workflow
                    def {pkg_name}(
                        sample_input: FlyteFile, output_dir: FlyteDirectory
                    ) -> str:
                        """Description...

                        {pkg_name} markdown
                        ----

                        Write some documentation about your workflow in
                        markdown here:

                        > Markdown syntax works as expected.

                        ## Foobar

                        __metadata__:
                            display_name: {pkg_name}
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
                        return {pkg_name}_task(
                            sample_input=sample_input,
                            output_dir=output_dir
                        )
                    '''
            )
        )

    version_f = pkg_root.joinpath("version")
    with open(version_f, "w") as f:
        f.write("0.0.0")
