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


@dataclass
class LPath:

    _cache: _Cache = field(
        repr=False,
        hash=False,
        compare=False,
    )

    path: str

    def __init__(self, path: Union[str, "LPath"]):
        if isinstance(path, LPath):
            path = path.path
        if not path.startswith("latch://"):
            raise ValueError(f"Invalid LPath: {path} is not a Latch path")
        self.path = path
        self._cache = _Cache()
        self._download_idx = 0

    def load(self) -> None:
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
                -1
                if meta["contentSize"] is None
                else int(final_link_target["ldataObjectMeta"]["contentSize"])
            )
            self._cache.content_type = final_link_target["ldataObjectMeta"][
                "contentType"
            ]

    def node_id(self, *, load_if_missing: bool = True) -> str:
        match = node_id_regex.match(self.path)
        if match:
            return match.group("id")

        if self._cache.node_id is None or self._cache.path != self.path:
            if not load_if_missing:
                return None
            self.load()
        return self._cache.node_id

    def name(self, *, load_if_missing: bool = True) -> str:
        if self._cache.name is None or self._cache.path != self.path:
            if not load_if_missing:
                return None
            self.load()
        return self._cache.name

    def type(self, *, load_if_missing: bool = True) -> LDataNodeType:
        if self._cache.type is None or self._cache.path != self.path:
            if not load_if_missing:
                return None
            self.load()
        return self._cache.type

    def size(self, *, load_if_missing: bool = True) -> float:
        if self._cache.size is None or self._cache.path != self.path:
            if not load_if_missing:
                return None
            self.load()
        return self._cache.size

    def content_type(self, *, load_if_missing: bool = True) -> str:
        if self._cache.content_type is None or self._cache.path != self.path:
            if not load_if_missing:
                return None
            self.load()
        return self._cache.content_type

    def is_dir(self) -> bool:
        return self.type() in dir_types

    def iterdir(self) -> Generator["LPath", None, None]:
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

    def copy(self, dst: "LPath", *, verbose: bool = False) -> None:
        _remote_copy(self.path, dst.path, verbose=verbose)

    def upload(self, src: Path, *, show_progress_bar: bool = False) -> None:
        _upload(
            os.fspath(src),
            self.path,
            progress=Progress.tasks if show_progress_bar else Progress.none,
            verbose=show_progress_bar,
        )

    def download(
        self, dst: Optional[Path] = None, *, show_progress_bar: bool = False
    ) -> Path:
        if dst is None:
            dir = Path.home() / "lpath" / str(self._download_idx)
            self._download_idx += 1
            dir.mkdir(parents=True, exist_ok=True)
            dst = dir / self.name()

        _download(
            self.path,
            dst,
            progress=Progress.tasks if show_progress_bar else Progress.none,
            verbose=show_progress_bar,
            confirm_overwrite=False,
        )
        return dst

    def __truediv__(self, other: object) -> "LPath":
        if not isinstance(other, (LPath, str)):
            return NotImplemented
        return LPath(urljoins(self.path, other))


class LPathTransformer(TypeTransformer[LPath]):
    _TYPE_INFO = BlobType(
        # there is no way to know if the LPath is a file or directory ahead to time,
        # so just set dimensionality to SINGLE
        format="binary",
        dimensionality=BlobType.BlobDimensionality.SINGLE,
    )

    def __init__(self):
        super(LPathTransformer, self).__init__(name="lpath-transformer", t=LPath)

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
