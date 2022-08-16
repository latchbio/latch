"""Service to execute a workflow in a container."""

from pathlib import Path
import os
from typing import Optional, Tuple, Generator
from distutils import dir_util
from shutil import copyfile

import docker.errors
from pyparsing import java_style_comment

from latch_cli.services.register import RegisterCtx, _print_build_logs, build_image
from flytekit.core.data_persistence import DataPersistence, DataPersistencePlugins

# Generate local paths to mock latch:/// and s3://
# TODO: Figure out how to do this without hard-coding mock_latch, mock_s3 paths
# For now, sticking with hard coded paths that probably won't be accessed by the user.
# Trying to dynamically create a random folder on the host and match up the paths between
# the host (the one that calls local_execute and establishes bind mounts) and the container
# is not straightforward and not worth the effort. It would only be protecting the user 
# from accidentally deleting some files in the mock folders. 
mock_latch_path = '/root/_mock_latch'
mock_s3_path = '/root/_mock_s3'

# Create local persistence class
class LocalExecutePersistence(DataPersistence):
    """
    The simplest form of persistence that is available with default flytekit - Disk-based persistence.
    This will store all data locally and retrieve the data from local. This is helpful for local execution and simulating
    runs.
    Local persistence to mock latch:/// and s3:// retrieval and storage during local execution.
    PROTOCOL: latch:/// or s3://
    """

    def __init__(self, default_prefix: Optional[str] = None, **kwargs):
        super().__init__(name='local-execute', default_prefix=default_prefix, **kwargs)

    @staticmethod
    def _make_local_path(path):
        if not os.path.exists(path):
            try:
                Path(path).mkdir(parents=True, exist_ok=True)
            except OSError:  # Guard against race condition
                if not os.path.isdir(path):
                    raise

    def strip_file_header(self, path: str) -> str:
        """
        Drops latch:/// or s3:// if it exists from the file
        """
        for protocol, prefix in [('latch://', mock_latch_path), 
                                 ('s3://', mock_s3_path)]:
            if path.startswith(protocol):
                return path.replace(protocol, prefix, 1)
        return path

    def listdir(self, path: str, recursive: bool = False) -> Generator[str, None, None]:
        if not recursive:
            files = os.listdir(self.strip_file_header(path))
            for f in files:
                yield f
            return

        for root, subdirs, files in os.walk(self.strip_file_header(path)):
            for f in files:
                yield os.path.join(root, f)
        return

    def exists(self, path: str):
        return os.path.exists(self.strip_file_header(path))

    def get(self, from_path: str, to_path: str, recursive: bool = False):
        if from_path != to_path:
            if recursive:
                dir_util.copy_tree(self.strip_file_header(from_path), self.strip_file_header(to_path))
            else:
                copyfile(self.strip_file_header(from_path), self.strip_file_header(to_path))

    def put(self, from_path: str, to_path: str, recursive: bool = False):
        if from_path != to_path:
            if recursive:
                dir_util.copy_tree(self.strip_file_header(from_path), self.strip_file_header(to_path))
            else:
                self._make_local_path(os.path.dirname(self.strip_file_header(to_path)))
                copyfile(self.strip_file_header(from_path), self.strip_file_header(to_path))

    def construct_path(self, _: bool, add_prefix: bool, *args: str) -> str:
        # Ignore add_protocol for now. Only complicates things
        if add_prefix:
            prefix = self.default_prefix if self.default_prefix else ""
            return os.path.join(prefix, *args)
        return os.path.join(*args)

def prep_container_for_local_exec() -> None:
    DataPersistencePlugins.register_plugin("latch://", LocalExecutePersistence, force=True)
    DataPersistencePlugins.register_plugin("s3://", LocalExecutePersistence, force=True)

def local_execute(
    pkg_root: Path, 
    use_auto_version: bool,
) -> None:
    """Executes a workflow locally within its latest registered container.

    Will stream in-container local execution stdout to terminal from which the
    subcommand is executed.

    Args:
        pkg_root: A path pointing to to the workflow package to be executed
            locally.
        use_auto_version: A bool indicating whether to use the default
            auto-versioning of the workflow. Recommended to be set to False for
            local execution so that previous images can be reused. Only really need
            to set to True for local execution if you update the Dockerfile.


    Example: ::

        $ latch local-execute myworkflow
        # Where `myworkflow` is a directory with workflow code.
    """
    ctx = RegisterCtx(pkg_root, disable_auto_version=(not use_auto_version))

    dockerfile = ctx.pkg_root.joinpath("Dockerfile")

    def _create_container(image_name: str):
        # Copy contents of workflow package to container's root directory to 
        # emulate natve workflow execution, rather than running from 
        # /root/local_execute, and rather than binding to /root.
        # Then call local_execute module to register the local latch/s3 plugins.
        cmd =   "cp -r /root/local_execute/!(mock_latch|mock_s3) /root ;" + \
                "python3 -c " + \
                "'from latch_cli.services.local_execute import prep_container_for_local_exec; " + \
                "prep_container_for_local_exec();" + \
                "from wf import main; main();'"
        container = ctx.dkr_client.create_container(
            image_name,
            command=["bash", "-O", "extglob", "-c", cmd],
            volumes=[str(ctx.pkg_root)],
            host_config=ctx.dkr_client.create_host_config(
                binds={
                    str(ctx.pkg_root): {
                        "bind": "/root/local_execute",
                        "mode": "ro",
                    },
                    str(ctx.pkg_root.joinpath('mock_latch')): {
                        "bind": mock_latch_path,
                        "mode": "rw",
                    },
                    str(ctx.pkg_root.joinpath('mock_s3')): {
                        "bind": mock_s3_path,
                        "mode": "rw",
                    },
                }
            ),
            working_dir="/root",
        )
        return container

    try:
        print("Spinning up local container...")
        print("NOTE ~ workflow code is bound as a mount.")
        print("You must register your workflow to persist changes.")

        container = _create_container(ctx.full_image_tagged)

    except docker.errors.ImageNotFound as e:
        print("Unable to find an image associated to this version of your workflow")
        print("Building from scratch:")

        build_logs = build_image(ctx, dockerfile)
        _print_build_logs(build_logs, ctx.full_image_tagged)

        container = _create_container(ctx.full_image_tagged)

    container_id = container.get("Id")

    ctx.dkr_client.start(container_id)
    logs = ctx.dkr_client.logs(container_id, stream=True)
    for x in logs:
        o = x.decode("utf-8")
        print(o, end="")
