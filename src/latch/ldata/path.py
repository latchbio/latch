import atexit
import os
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator, Optional, Type

import gql
from flytekit import (
    Blob,
    BlobMetadata,
    BlobType,
    FlyteContext,
    Literal,
    LiteralType,
    Scalar,
)
from flytekit.extend import TypeEngine, TypeTransformer
from typing_extensions import Self

from latch.ldata.type import LatchPathError, LDataNodeType
from latch_cli.utils import urljoins

from ._transfer.download import download as _download
from ._transfer.node import get_node_data as _get_node_data
from ._transfer.progress import Progress as _Progress
from ._transfer.remote_copy import remote_copy as _remote_copy
from ._transfer.upload import upload as _upload
from ._transfer.utils import query_with_retry

node_id_regex = re.compile(r"^latch://(?P<id>[0-9]+)\.node$")


_dir_types = {
    LDataNodeType.dir,
    LDataNodeType.account_root,
    LDataNodeType.mount,
    LDataNodeType.mount_gcp,
    LDataNodeType.mount_azure,
}

_download_idx = 0


@dataclass
class _Cache:
    path: Optional[str] = None
    node_id: Optional[str] = None
    name: Optional[str] = None
    type: Optional[LDataNodeType] = None
    size: Optional[int] = None
    dir_size: Optional[int] = None
    content_type: Optional[str] = None


@dataclass(frozen=True)
class LPath:
    """Latch Path.

    Represents a remote file/directory path hosted on Latch. Can be used to
    interact with files and directories in Latch.

    Attributes:
    path: The Latch path. Must start with "latch://".
    """

    _cache: _Cache = field(
        default_factory=_Cache,
        init=False,
        repr=False,
        hash=False,
        compare=False,
    )

    path: str

    def __post_init__(self):
        if isinstance(self.path, LPath):
            raise ValueError("LPath cannot be initialized with another LPath")
        if not self.path.startswith("latch://"):
            raise ValueError(f"invalid LPath: {self.path} is not a Latch path")

    def fetch_metadata(self) -> None:
        """(Re-)populate this LPath's instance's cache.

        Future calls to most getters will return immediately without making a network request.

        Always makes a network request.
        """
        data = query_with_retry(
            gql.gql("""
            query GetNodeData($path: String!) {
                ldataResolvePathToNode(path: $path) {
                    path
                    ldataNode {
                        finalLinkTarget {
                            id
                            name
                            type
                            ldataObjectMeta {
                                contentSize
                                contentType
                            }
                        }
                    }
                }
            }"""),
            {"path": self.path},
        )["ldataResolvePathToNode"]

        if (
            data is None
            or data["ldataNode"] is None
            or (data["path"] is not None and data["path"] != "")
        ):
            raise LatchPathError(f"no such Latch file or directory", self.path)

        self._clear_cache()

        self._cache.path = self.path

        final_link_target = data["ldataNode"]["finalLinkTarget"]
        self._cache.node_id = final_link_target["id"]
        self._cache.name = final_link_target["name"]
        self._cache.type = LDataNodeType(final_link_target["type"].lower())

        meta = final_link_target["ldataObjectMeta"]
        if meta is not None:
            self._cache.size = (
                None if meta["contentSize"] is None else int(meta["contentSize"])
            )
            self._cache.content_type = meta["contentType"]

    def _clear_cache(self):
        self._cache.path = None
        self._cache.node_id = None
        self._cache.name = None
        self._cache.type = None
        self._cache.size = None
        self._cache.dir_size = None
        self._cache.content_type = None

    def node_id(self, *, load_if_missing: bool = True) -> Optional[str]:
        match = node_id_regex.match(self.path)
        if match:
            return match.group("id")

        if self._cache.node_id is None and load_if_missing:
            self.fetch_metadata()
        return self._cache.node_id

    def name(self, *, load_if_missing: bool = True) -> Optional[str]:
        if self._cache.name is None and load_if_missing:
            self.fetch_metadata()
        return self._cache.name

    def type(self, *, load_if_missing: bool = True) -> Optional[LDataNodeType]:
        if self._cache.type is None and load_if_missing:
            self.fetch_metadata()
        return self._cache.type

    def size_recursive(self, *, load_if_missing: bool = True) -> Optional[int]:
        if not self.is_dir(load_if_missing=load_if_missing):
            return self.size(load_if_missing=load_if_missing)

        if self._cache.dir_size is None and load_if_missing:
            data = query_with_retry(
                gql.gql("""
                query GetLDataSubtreeSize($nodeId: BigInt!) {
                    ldataGetSubtreeSizeRecursive(argNodeId: $nodeId)
                }
                """),
                {"nodeId": self.node_id()},
                num_retries=2,
            )["ldataGetSubtreeSizeRecursive"]
            self._cache.dir_size = int(data)

        return self._cache.dir_size

    def size(self, *, load_if_missing: bool = True) -> Optional[int]:
        if self._cache.size is None and load_if_missing:
            self.fetch_metadata()
        return self._cache.size

    def content_type(self, *, load_if_missing: bool = True) -> Optional[str]:
        if self._cache.content_type is None and load_if_missing:
            self.fetch_metadata()
        return self._cache.content_type

    def is_dir(self, *, load_if_missing: bool = True) -> bool:
        return self.type(load_if_missing=load_if_missing) in _dir_types

    def iterdir(self) -> Iterator[Self]:
        """Yield LPaths objects contained within the directory.

        Should only be called on directories. Does not recursively list directories.

        Always makes a network request.
        """
        data = query_with_retry(
            gql.gql("""
            query LDataChildren($argPath: String!) {
                ldataResolvePathData(argPath: $argPath) {
                    finalLinkTarget {
                        type
                        childLdataTreeEdges(filter: { child: { removed: { equalTo: false } } }) {
                            nodes {
                                child {
                                    name
                                }
                            }
                        }
                    }
                }
            }"""),
            {"argPath": self.path},
        )["ldataResolvePathData"]

        if data is None:
            raise LatchPathError(f"no such Latch file or directory", {self.path})
        if data["finalLinkTarget"]["type"].lower() not in _dir_types:
            raise ValueError(f"not a directory: {self.path}")

        for node in data["finalLinkTarget"]["childLdataTreeEdges"]["nodes"]:
            yield LPath(urljoins(self.path, node["child"]["name"]))

    def mkdirp(self) -> None:
        node = _get_node_data(self.path, allow_resolve_to_parent=True).data[self.path]
        if node.exists():
            if node.type not in _dir_types:
                raise ValueError(f"{self.path} exists and is not a directory")
            return

        path = f"latch://{node.id}.node/{node.remaining}"
        path += "/" if not path.endswith("/") else ""
        query_with_retry(
            gql.gql("""
            mutation LDataMkdirP($path: String!) {
                ldataMkdirp(input: { argPath: $path }) {
                    bigInt
                }
            }
            """),
            {"path": path},
        )
        self._clear_cache()

    def rmr(self) -> None:
        """Recursively delete files at this instance's path.

        Throws LatchPathError if the path does not exist.

        Always makes a network request.
        """
        query_with_retry(
            gql.gql("""
            mutation LDataRmr($nodeId: BigInt!) {
                ldataRmr(input: { argNodeId: $nodeId }) {
                    clientMutationId
                }
            }
            """),
            {"nodeId": self.node_id()},
        )
        self._clear_cache()

    def copy_to(self, dst: "LPath") -> None:
        """Copy the file at this instance's path to the given destination.

        Args:
        dst: The destination LPath.
        show_summary: Whether to print a summary of the copy operation.
        """
        _remote_copy(self.path, dst.path, create_parents=True)

    def upload_from(self, src: Path, *, show_progress_bar: bool = False) -> None:
        """Upload the file at the given source to this instance's path.

        Args:
        src: The source path.
        show_progress_bar: Whether to show a progress bar during the upload.
        """
        _upload(
            os.fspath(src),
            self.path,
            progress=_Progress.tasks if show_progress_bar else _Progress.none,
            verbose=False,
            create_parents=True,
        )
        self._clear_cache()

    def download(
        self, dst: Optional[Path] = None, *, show_progress_bar: bool = False
    ) -> Path:
        """Download the file at this instance's path to the given destination.

        Args:
        dst: The destination path. If None, a temporary directory is created and the file is
            downloaded there. The temprary directory is deleted when the program exits.
        show_progress_bar: Whether to show a progress bar during the download.
        """
        if dst is None:
            global _download_idx
            tmp_dir = Path.home() / ".latch" / "lpath" / str(_download_idx)
            _download_idx += 1
            tmp_dir.mkdir(parents=True, exist_ok=True)
            atexit.register(lambda p: shutil.rmtree(p), tmp_dir)
            dst = tmp_dir / self.name()

        _download(
            self.path,
            dst,
            progress=_Progress.tasks if show_progress_bar else _Progress.none,
            verbose=False,
            confirm_overwrite=False,
        )
        return dst

    def __truediv__(self, other: object) -> "LPath":
        if not isinstance(other, str):
            return NotImplemented
        return LPath(urljoins(self.path, other))


class LPathTransformer(TypeTransformer[LPath]):
    _TYPE_INFO = BlobType(
        # todo(rahul): there is no way to know if the LPath is a file or directory
        # ahead to time, so just set dimensionality to SINGLE
        format="",
        dimensionality=BlobType.BlobDimensionality.SINGLE,
    )

    def __init__(self):
        super().__init__(name="lpath-transformer", t=LPath)

    def get_literal_type(self, t: Type[LPath]) -> LiteralType:
        return LiteralType(blob=self._TYPE_INFO)

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: LPath,
        python_type: Type[LPath],
        expected: LiteralType,
    ) -> Literal:
        return Literal(
            scalar=Scalar(
                blob=Blob(
                    uri=python_val.path, metadata=BlobMetadata(type=self._TYPE_INFO)
                )
            )
        )

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[LPath]
    ):
        return LPath(path=lv.scalar.blob.uri)


TypeEngine.register(LPathTransformer())
