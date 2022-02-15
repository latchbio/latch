from enum import Enum
from os import PathLike
from typing import Annotated, Union
from urllib.parse import urlparse

# Note this only exists in flaightkit fork.
from flytekit.core.with_metadata import FlyteMetadata
from flytekit.types.file import FlyteFile


class LatchSchemes(Enum):
    latch = "latch"
    s3 = "s3"


class URL:
    """Validates a URL string with respect to a scheme.

    Args:
        scheme : eg. s3, latch
        raw_url : the url string to be validated
    """

    def __init__(self, scheme: str, raw_url: str):
        scheme = scheme.value
        raw_scheme = urlparse(raw_url).scheme
        if raw_scheme != scheme:
            raise ValueError(f"{raw_url} is must use the {scheme} scheme.")
        self._url = raw_url

    @property
    def url(self) -> str:
        """Returns self as string."""
        return self._url


class LatchURL(URL):
    """A URL referencing an object in LatchData.

    Uses the latch scheme and a path that resolves absolutely with
    respect to an authenticated users's root.

    ..
        latch:///foobar # a valid directory
        latch:///test_samples/test.fa # a valid file
    """

    def __init__(self, raw_url: str):
        super().__init__(LatchSchemes.latch, raw_url)


class S3URL:
    """A URL referencing an object in S3.

    ..
        s3:/<bucket>//path
    """

    def __init__(self, raw_url: str):
        super().__init__(LatchSchemes.s3, raw_url)


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

    local_path: str = None
    """File path local to the environment executing the task."""

    remote_path: str = None
    """A url referencing in object in LatchData or s3."""

    def __init__(self, path: Union[str, PathLike], remote_path: PathLike, **kwargs):

        remote_path = LatchURL(remote_path).url  # validation

        if kwargs.get("downloader") is not None:
            super().__init__(path, kwargs["downloader"], remote_path)
        else:
            super().__init__(path, None, remote_path)

        # This will manually download object to local disk in the case of a
        # user wishing to access self locally without referencing the path
        # through `__fspath__`, eg. through `self.local_path`.
        self.local_path = self.__fspath__()

        # self.remote_path already constructed.


LatchOutputFile = Annotated[
    LatchFile,
    FlyteMetadata(
        {"output": True},
    ),
]
"""A LatchFile tagged as the output of some workflow.

The Latch Console uses this metadata to avoid checking for existence of the
file at its remote path and displaying an error. This check is normally made to
avoid launching workflows with LatchFiles that point to objects that don't
exist.
"""
