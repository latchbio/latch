import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Generator, Optional, Type, Union

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
from latch_sdk_gql.execute import execute

from latch.ldata.transfer.node import LDataNodeType
from latch.ldata.transfer.progress import Progress
from latch_cli.utils import urljoins

from .transfer.download import _download
from .transfer.remote_copy import _remote_copy
from .transfer.upload import _upload

node_id_regex = re.compile(r"^latch://(?P<id>[0-9]+)\.node$")


dir_types = {
    LDataNodeType.dir,
    LDataNodeType.account_root,
    LDataNodeType.mount,
}


@dataclass
class _Cache:
    """Internal cache class to organize information for a `LPath`."""

    path: Optional[str] = None
    node_id: Optional[str] = None
    name: Optional[str] = None
    type: Optional[LDataNodeType] = None
    size: Optional[int] = None
    content_type: Optional[str] = None


download_idx = 0


@dataclass(frozen=True)
class LPath:

    _cache: _Cache = field(
        default_factory=lambda: _Cache(),
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
            raise ValueError(f"Invalid LPath: {self.path} is not a Latch path")

    def load_metadata(self) -> None:
        """(Re-)populate this LPath's instance's cache.

        Future calls to most getters will return immediately without making a network request.

        Always makes a network request.
        """
        data = execute(
            gql.gql("""
            query GetNodeData($path: String!) {
                ldataResolvePathToNode(path: $path) {
                    path
                    ldataNode {
                        finalLinkTarget {
                            id
                            name
                            type
                            removed
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

        if data is None or data["ldataNode"] is None:
            raise FileNotFoundError(f"No such Latch file or directory: {self.path}")

        self._cache.path = self.path

        final_link_target = data["ldataNode"]["finalLinkTarget"]
        self._cache.node_id = final_link_target["id"]
        self._cache.name = final_link_target["name"]
        self._cache.type = LDataNodeType(final_link_target["type"].lower())

        meta = final_link_target["ldataObjectMeta"]
        if meta is not None:
            self._cache.size = (
                -1 if meta["contentSize"] is None else int(meta["contentSize"])
            )
            self._cache.content_type = meta["contentType"]

    def node_id(self, *, load_if_missing: bool = True) -> Optional[str]:
        match = node_id_regex.match(self.path)
        if match:
            return match.group("id")

        if self._cache.node_id is None and load_if_missing:
            self.load_metadata()
        return self._cache.node_id

    def name(self, *, load_if_missing: bool = True) -> Optional[str]:
        if self._cache.name is None and load_if_missing:
            self.load_metadata()
        return self._cache.name

    def type(self, *, load_if_missing: bool = True) -> Optional[LDataNodeType]:
        if self._cache.type is None and load_if_missing:
            self.load_metadata()
        return self._cache.type

    def size(self, *, load_if_missing: bool = True) -> Optional[int]:
        if self._cache.size is None and load_if_missing:
            self.load_metadata()
        return self._cache.size

    def content_type(self, *, load_if_missing: bool = True) -> Optional[str]:
        if self._cache.content_type is None and load_if_missing:
            self.load_metadata()
        return self._cache.content_type

    def is_dir(self, *, load_if_missing: bool = True) -> bool:
        return self.type(load_if_missing=load_if_missing) in dir_types

    def iterdir(self) -> Generator["LPath", None, None]:
        """Yield LPaths objects contained within the directory.

        Should only be called on directories. Does not recursively list directories.

        Always makes a network request.
        """
        data = execute(
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
            raise FileNotFoundError(f"No such Latch file or directory: {self.path}")
        if data["finalLinkTarget"]["type"].lower() not in dir_types:
            raise ValueError(f"{self.path} is not a directory")

        for node in data["finalLinkTarget"]["childLdataTreeEdges"]["nodes"]:
            yield LPath(urljoins(self.path, node["child"]["name"]))

    def rmr(self) -> None:
        """Recursively delete files at this instance's path.

        Always makes a network request.
        """
        execute(
            gql.gql("""
            mutation LDataRmr($nodeId: BigInt!) {
                ldataRmr(input: { argNodeId: $nodeId }) {
                    clientMutationId
                }
            }
            """),
            {"nodeId": self.node_id},
        )

    def copy_to(self, dst: "LPath", *, show_summary: bool = False) -> None:
        _remote_copy(self.path, dst.path, show_summary=show_summary)

    def upload_from(self, src: Path, *, show_progress_bar: bool = False) -> None:
        _upload(
            os.fspath(src),
            self.path,
            progress=Progress.tasks if show_progress_bar else Progress.none,
            verbose=False,
        )

    def download(
        self, dst: Optional[Path] = None, *, show_progress_bar: bool = False
    ) -> Path:
        if dst is None:
            global download_idx
            dir = Path.home() / "lpath"
            dir.mkdir(parents=True, exist_ok=True)
            dst = dir / f"{download_idx}_{self.name()}"
            download_idx += 1

        _download(
            self.path,
            dst,
            progress=Progress.tasks if show_progress_bar else Progress.none,
            verbose=False,
            confirm_overwrite=False,
        )
        return dst

    def __truediv__(self, other: object) -> "LPath":
        if not isinstance(other, (LPath, str)):
            return NotImplemented
        return LPath(urljoins(self.path, other))


class LPathTransformer(TypeTransformer[LPath]):
    _TYPE_INFO = BlobType(
        # rahul: there is no way to know if the LPath is a file or directory
        # ahead to time, so just set dimensionality to SINGLE
        format="binary",
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
