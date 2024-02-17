from pathlib import Path
from typing import Generator, Optional, Type, Union
from urllib.parse import urljoin
from uuid import uuid4

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
from latch_sdk_config.latch import NUCLEUS_URL
from latch_sdk_gql.execute import execute

from latch.ldata.node import (
    LDataNodeType,
    LDataPerms,
    PermLevel,
    get_node_data,
    get_node_metadata,
    get_node_perms,
)
from latch.ldata.transfer import download, remote_copy, upload
from latch.ldata.transfer.progress import Progress
from latch_cli.tinyrequests import post
from latch_cli.utils import get_auth_header, urljoins
from latch_cli.utils.path import is_remote_path


class LPath:
    def __init__(self, path: str):
        if not is_remote_path(path):
            raise ValueError(f"Invalid LPath: {path} is not a Latch path")
        self.path = path
        self._node_id = None

    @property
    def node_id(self) -> str:
        if self._node_id is None:
            self._node_id = get_node_data(self.path).data[self.path].id
        return self._node_id

    @property
    def exists(self) -> bool:
        try:
            node_data = get_node_data(self.path).data[self.path]
        except FileNotFoundError:
            return False
        return not node_data.removed

    @property
    def name(self) -> str:
        return get_node_data(self.path).data[self.path].name

    @property
    def type(self) -> LDataNodeType:
        return get_node_data(self.path).data[self.path].type

    def is_dir(self) -> bool:
        return self.type in {
            LDataNodeType.dir,
            LDataNodeType.account_root,
            LDataNodeType.mount,
        }

    @property
    def size(self) -> float:
        metadata = get_node_metadata(self.node_id)
        return metadata.size

    @property
    def content_type(self) -> str:
        metadata = get_node_metadata(self.node_id)
        return metadata.content_type

    def iterdir(self) -> Generator[Path, None, None]:
        if not self.is_dir():
            raise ValueError(f"Not a directory: {self.path}")
        data = execute(
            gql.gql("""
            query LDataChildren($argPath: String!) {
                ldataResolvePathData(argPath: $argPath) {
                    finalLinkTarget {
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
            raise ValueError(f"No directory found at path: {self.path}")

        for node in data["finalLinkTarget"]["childLdataTreeEdges"]["nodes"]:
            yield urljoins(self.path, node["child"]["name"])

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

    def copy(self, dst: Union["LPath", str]) -> None:
        remote_copy(self.path, str(dst))

    def upload(self, src: Path, progress=Progress.tasks, verbose=False) -> None:
        upload(src, self.path, progress, verbose)

    def download(
        self, dst: Optional[Path] = None, progress=Progress.tasks, verbose=False
    ) -> Path:
        if dst is None:
            dir = Path(".") / "downloads" / str(uuid4())
            dir.mkdir(parents=True, exist_ok=True)
            dst = dir / self.name

        download(self.path, dst, progress, verbose, confirm_overwrite=False)
        return dst

    @property
    def perms(self) -> LDataPerms:
        return get_node_perms(self.node_id)

    def share_with(self, email: str, perm_level: PermLevel) -> None:
        resp = post(
            url=urljoin(NUCLEUS_URL, "/ldata/send-share-email"),
            json={
                "node_id": self.node_id,
                "perm_level": str(perm_level),
                "receiver_email": email,
            },
            headers={"Authorization": get_auth_header()},
        )
        resp.raise_for_status()

    def _toggle_share_link(self, enable: bool) -> None:
        execute(
            gql.gql("""
            mutation LDataShare($nodeId: BigInt!, $value: Boolean!) {
                ldataShare(input: { argNodeId: $nodeId, argValue: $value }) {
                    clientMutationId
                }
            }
            """),
            {"nodeId": self.node_id, "value": enable},
        )

    def enable_share_link(self) -> None:
        self._toggle_share_link(True)

    def disable_share_link(self) -> None:
        self._toggle_share_link(False)

    def __str__(self) -> str:
        return self.path

    def __truediv__(self, other: Union[Path, str]) -> "LPath":
        return LPath(f"{Path(self.path) / other}")


class LPathTransformer(TypeTransformer[LPath]):
    def __init__(self):
        super(LPathTransformer, self).__init__(name="lpath-transformer", t=LPath)

    def get_literal_type(self, t: Type[LPath]) -> LiteralType:
        return LiteralType(
            blob=BlobType(
                # this is sus, but there is no way to check if the LPath is a file or dir
                format="binary",
                dimensionality=BlobType.BlobDimensionality.SINGLE,
            )
        )

    def to_literal(
        self,
        ctx: FlyteContext,
        python_val: LPath,
        python_type: Type[LPath],
        expected: LiteralType,
    ) -> Literal:
        dimensionality = (
            BlobType.BlobDimensionality.MULTIPART
            if python_val.is_dir()
            else BlobType.BlobDimensionality.SINGLE
        )
        return Literal(
            scalar=Scalar(
                blob=Blob(
                    uri=python_val.path,
                    metadata=BlobMetadata(
                        format="binary", dimensionality=dimensionality
                    ),
                )
            )
        )

    def to_python_value(
        self, ctx: FlyteContext, lv: Literal, expected_python_type: Type[LPath]
    ):
        return LPath(path=lv.scalar.blob.uri)


TypeEngine.register(LPathTransformer())
