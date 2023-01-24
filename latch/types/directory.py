from os import PathLike
from typing import Annotated, Optional, Type, Union, get_args, get_origin

from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal
from flytekit.types.directory.types import (
    FlyteDirectory,
    FlyteDirToMultipartBlobTransformer,
)

from latch.types.utils import _is_valid_url

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated


class LatchDir(FlyteDirectory):
    """Represents a directory in the context of a task execution.

    The local path identifies the directory location on local disk in
    the context of a task execution.

    ..
        @task
        def task(dir: LatchDir):

            mypath = Path(dir).joinpath("foo.txt").resolve()


    The remote path identifies a remote location. The remote location, either a
    latch or s3 url, can be inspected from an object passed to the task to
    reveal its remote source.

    It can also to deposit the file to a latch path when the directory is
    returned from a task.

    ..

        @task
        def task(dir: LatchFile):


            # Manipulate directory contents locally and return back to
            # LatchData

            return LatchDir("./foo", "latch:///foo")
    """

    def __init__(
        self,
        path: Union[str, PathLike],
        remote_path: Optional[PathLike] = None,
        **kwargs,
    ):

        if path is None:
            raise ValueError("Unable to instantiate LatchDir with None")

        # Cast PathLike objects so that LatchDir has consistent JSON
        # representation.
        self.path = str(path)

        if _is_valid_url(path) and remote_path is None:
            self._remote_directory = str(path)
        else:
            self._remote_directory = None if remote_path is None else str(remote_path)

        if kwargs.get("downloader") is not None:
            super().__init__(path, kwargs["downloader"], remote_path)
        else:

            def downloader():
                ctx = FlyteContextManager.current_context()
                if (
                    ctx is not None
                    and hasattr(self, "_remote_directory")
                    and self._remote_directory is not None
                    # todo(kenny) is this necessary?
                    and ctx.inspect_objects_only is False
                ):
                    self.path = ctx.file_access.get_random_local_directory()
                    return ctx.file_access.get_data(
                        self._remote_directory,
                        self.path,
                        is_multipart=True,
                    )

            super().__init__(path, downloader, self._remote_directory)

    @property
    def local_path(self) -> str:
        """File path local to the environment executing the task."""

        # This will manually download object to local disk in the case of a
        # user wishing to access the file without using `open`.
        self.__fspath__()

        return self.path

    @property
    def remote_path(self) -> Optional[str]:
        """A url referencing in object in LatchData or s3."""
        return self._remote_directory

    def __repr__(self):
        if self.remote_path is None:
            return f'LatchDir("{self.local_path}")'
        return f'LatchDir("{self.path}", remote_path="{self.remote_path}")'

    def __str__(self):
        if self.remote_path is None:
            return "LatchDir()"
        return f'LatchDir("{self.remote_path}")'


LatchOutputDir = Annotated[
    LatchDir,
    FlyteAnnotation(
        {"output": True},
    ),
]
"""A LatchDir tagged as the output of some workflow.

The Latch Console uses this metadata to avoid checking for existence of the
file at its remote path and displaying an error. This check is normally made to
avoid launching workflows with LatchDirs that point to objects that don't
exist.
"""


class LatchDirPathTransformer(FlyteDirToMultipartBlobTransformer):
    def __init__(self):
        TypeTransformer.__init__(self, name="LatchDirPath", t=LatchDir)

    def to_python_value(
        self,
        ctx: FlyteContext,
        lv: Literal,
        expected_python_type: Union[Type[LatchDir], PathLike],
    ) -> FlyteDirectory:

        uri = lv.scalar.blob.uri
        if expected_python_type is PathLike:
            raise TypeError(
                "Casting from Pathlike to LatchDir is currently not supported."
            )

        
        while get_origin(expected_python_type) == Annotated:
            expected_python_type = get_args(expected_python_type)[0]

        if not issubclass(expected_python_type, LatchDir):
            raise TypeError(
                f"Neither os.PathLike nor LatchDir specified {expected_python_type}"
            )

        # This is a local file path, like /usr/local/my_file, don't mess with it. Certainly, downloading it doesn't
        # make any sense.
        if not ctx.file_access.is_remote(uri):
            return expected_python_type(uri)

        # For the remote case, return an FlyteDirectory object that can download
        local_folder = ctx.file_access.get_random_local_directory()

        def _downloader():
            return ctx.file_access.get_data(uri, local_folder, is_multipart=True)

        ret = LatchDir(local_folder, uri, downloader=_downloader)
        ret._remote_source = uri
        return ret


TypeEngine.register(LatchDirPathTransformer())
