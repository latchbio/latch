from os import PathLike
from typing import Optional, Type, Union

from flytekit.core.context_manager import FlyteContext
# Note this only exists in flaightkit fork.
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.core.with_metadata import FlyteMetadata
from flytekit.models.literals import Literal
from flytekit.types.directory.types import (FlyteDirectory,
                                            FlyteDirToMultipartBlobTransformer)
from latch.types.url import LatchURL

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
        self, path: Union[str, PathLike], remote_path: PathLike = None, **kwargs
    ):

        if remote_path is not None:
            self._remote_directory = LatchURL(
                remote_path).url  # validates url string

        if kwargs.get("downloader") is not None:
            super().__init__(path, kwargs["downloader"], remote_path)
        else:

            def noop():
                ...

            super().__init__(path, noop, remote_path)

        # This will manually download object to local disk in the case of a
        # user wishing to access self locally without referencing the path
        # through `__fspath__`, eg. through `self.local_path`.
        self.__fspath__()

    @property
    def local_path(self) -> str:
        """File path local to the environment executing the task."""
        return self._path

    @property
    def remote_path(self) -> Optional[str]:
        """A url referencing in object in LatchData or s3."""
        return self._remote_directory

    def __str__(self):
        return f'LatchDir("{self.local_path}")'


LatchOutputDir = Annotated[
    LatchDir,
    FlyteMetadata(
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
            return ctx.file_access.get_data(uri, local_folder, is_multipart=False)

        return LatchDir(local_folder, uri, downloader=_downloader)


TypeEngine.register(LatchDirPathTransformer())
