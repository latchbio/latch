from os import PathLike
from typing import Optional, Type, Union

from latch_cli.services.cp import _download

try:
    from typing import Annotated
except ImportError:
    from typing_extensions import Annotated

from flytekit.core.annotation import FlyteAnnotation
from flytekit.core.context_manager import FlyteContext, FlyteContextManager
from flytekit.core.type_engine import TypeEngine, TypeTransformer
from flytekit.models.literals import Literal
from flytekit.types.file.file import FlyteFile, FlyteFilePathTransformer

from latch.types.utils import _is_valid_url


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
        remote_path: Optional[PathLike] = None,
        **kwargs,
    ):
        if _is_valid_url(path) and remote_path is None:
            self._remote_path = path
        else:
            self._remote_path = remote_path

        if kwargs.get("downloader") is not None:
            super().__init__(path, kwargs["downloader"], remote_path)
        else:

            def downloader():
                ctx = FlyteContextManager.current_context()
                if (
                    ctx is not None
                    and hasattr(self, "_remote_path")
                    and self._remote_path is not None
                ):
                    self.path = ctx.file_access.get_random_local_path(self._remote_path)
                    return ctx.file_access.get_data(
                        self._remote_path,
                        self.path,
                        is_multipart=False,
                    )

            super().__init__(path, downloader, self._remote_path)

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
        return self._remote_path

    def __str__(self):
        return f'LatchFile("{self.remote_path}")'


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
    ) -> FlyteFile:

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
