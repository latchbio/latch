import os
import re
from os import PathLike
from pathlib import Path
from typing import Optional, Type, Union
from urllib.parse import urlparse

import gql
from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer
from latch_sdk_gql.execute import execute
from typing_extensions import Annotated

from latch.ldata.path import LPath
from latch.types.utils import format_path, is_absolute_node_path, is_valid_url
from latch_cli.utils.path import normalize_path


class LatchFile(FlyteFile):
    """Represents a file object in the context of a task execution.

    The local path identifies the file object's location on local disk in
    the context of a task execution. `LatchFile` inherits implementation of
    `__fsopen__` from `FlyteFile`, so methods like `open` can retrieve a string
    representation of self.

    ..
        @task
        def task(file: LatchFile):

            with open(file, "r") as f:
                print(f.read())

            mypath = Path(file).resolve()


    The remote path identifies a remote location. The remote location, either a
    latch or s3 url, can be inspected from an object passed to the task to
    reveal its remote source.

    It can also to deposit the file to a latch path when the object is returned
    from a task.

    ..

        @task
        def task(file: LatchFile):

            path = file.remote_path # inspect remote location

            # Returning a different file to LatchData.
            return LatchFile("./foobar.txt", "latch:///foobar.txt")
    """

    def __init__(
        self,
        path: Union[str, PathLike],
        remote_path: Optional[Union[str, PathLike]] = None,
        **kwargs,
    ):
        if path is None:
            raise ValueError("Unable to instantiate LatchFile with None")

        # Cast PathLike objects so that LatchFile has consistent JSON
        # representation.
        parsed = urlparse(str(path))
        if parsed.scheme == "file":
            self.path = parsed.path
        elif parsed.scheme == "latch":
            self.path = normalize_path(str(path))
        else:
            self.path = str(path)

        self._path_generated = False

        if is_valid_url(self.path) and remote_path is None:
            self._remote_path = str(path)
        else:
            self._remote_path = None if remote_path is None else str(remote_path)

        if kwargs.get("downloader") is not None:
            super().__init__(self.path, kwargs["downloader"], self._remote_path)
        else:

            def downloader():
                ctx = FlyteContextManager.current_context()
                if (
                    ctx is not None
                    and hasattr(self, "_remote_path")
                    and self._remote_path is not None
                    # todo(kenny) is this necessary?
                    and ctx.inspect_objects_only is False
                ):
                    local_path_hint = self._remote_path
                    if is_absolute_node_path.match(self._remote_path) is not None:
                        data = execute(
                            gql.gql("""
                            query getName($argPath: String!) {
                                ldataResolvePathData(argPath: $argPath) {
                                    name
                                }
                            }
                            """),
                            {"argPath": self._remote_path},
                        )["ldataResolvePathData"]

                        if data is not None and data["name"] is not None:
                            local_path_hint = data["name"]

                    self._idempotent_set_path(local_path_hint)

                    return ctx.file_access.get_data(
                        self._remote_path,
                        self.path,
                        is_multipart=False,
                    )

            super().__init__(self.path, downloader, self._remote_path)

    def size(self):
        return LPath(self.remote_path).size()

    def _idempotent_set_path(self, hint: Optional[str] = None):
        if self._path_generated:
            return

        ctx = FlyteContextManager.current_context()
        if ctx is None:
            return

        self.path = ctx.file_access.get_random_local_path(hint)
        self._path_generated = True

    def _create_imposters(self):
        self._idempotent_set_path()

        p = Path(self.path)
        p.parent.mkdir(exist_ok=True, parents=True)
        p.touch(exist_ok=True)

    @property
    def local_path(self) -> str:
        """File path local to the environment executing the task."""

        # This will manually download object to local disk in the case of a
        # user wishing to access the file without using `open`.
        self.__fspath__()

        return str(self.path)

    @property
    def remote_path(self) -> Optional[str]:
        """A url referencing in object in LatchData or s3."""
        return self._remote_path

    def __repr__(self):
        if self.remote_path is None:
            return f"LatchFile({repr(format_path(self.local_path))})"

        return (
            f"LatchFile({repr(self.path)},"
            f" remote_path={repr(format_path(self.remote_path))})"
        )

    def __str__(self):
        if self.remote_path is None:
            return "LatchFile()"
        return f"LatchFile({format_path(self.remote_path)})"


LatchOutputFile = Annotated[
    LatchFile,
    FlyteAnnotation(
        {"output": True},
    ),
]
"""A LatchFile tagged as the output of some workflow.

The Latch Console uses this metadata to avoid checking for existence of the
file at its remote path and displaying an error. This check is normally made to
avoid launching workflows with LatchFiles that point to objects that don't
exist.
"""


class LatchFilePathTransformer(FlyteFilePathTransformer):
    def __init__(self):
        TypeTransformer.__init__(self, name="LatchFilePath", t=LatchFile)

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Union[Type[LatchFile], PathLike],
    ) -> LatchFile:
        uri = lv.scalar.blob.uri
        if expected_python_type is PathLike:
            raise TypeError(
                "Casting from Pathlike to LatchFile is currently not supported."
            )

        if not issubclass(expected_python_type, LatchFile):
            raise TypeError(
                f"Neither os.PathLike nor LatchFile specified {expected_python_type}"
            )

        # This is a local file path, like /usr/local/my_file, don't mess with it. Certainly, downloading it doesn't
        # make any sense.
        if not ctx.file_access.is_remote(uri):
            return expected_python_type(uri)

        # For the remote case, return an FlyteFile object that can download
        local_path = ctx.file_access.get_random_local_path(uri)

        def _downloader():
            return ctx.file_access.get_data(uri, local_path, is_multipart=False)

        ret = LatchFile(local_path, uri, downloader=_downloader)
        ret._remote_source = uri
        return ret


TypeEngine.register(LatchFilePathTransformer())
