import io
from pathlib import Path
from typing import Generator, Optional, Union
from urllib.parse import urljoin

import gql
from gql.transport.exceptions import TransportQueryError
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
from latch.ldata.transfer import download, upload
from latch.types.json import JsonValue
from latch_cli.tinyrequests import post
from latch_cli.utils import get_auth_header, urljoins
from latch_cli.utils.path import get_name_from_path, get_path_error, is_remote_path


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
        return self.type is LDataNodeType.dir

    @property
    def size(self) -> float:
        metadata = get_node_metadata(self.node_id)
        return metadata.size

    @property
    def content_type(self) -> str:
        metadata = get_node_metadata(self.node_id)
        return metadata.content_type

    def iterdir(self) -> Generator[Path, None, None]:
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
        dst = str(dst)
        node_data = get_node_data(self.path, dst, allow_resolve_to_parent=True)

        src_data = node_data.data[self.path]
        dst_data = node_data.data[dst]
        acc_id = node_data.acc_id

        path_by_id = {v.id: k for k, v in node_data.data.items()}

        if src_data.is_parent:
            raise FileNotFoundError(get_path_error(self.path, "not found", acc_id))

        new_name = None
        if dst_data.is_parent:
            new_name = get_name_from_path(dst)
        elif dst_data.type in {LDataNodeType.obj, LDataNodeType.link}:
            raise FileExistsError(
                get_path_error(dst, "object already exists at path.", acc_id)
            )

        try:
            execute(
                gql.gql("""
                mutation Copy(
                    $argSrcNode: BigInt!
                    $argDstParent: BigInt!
                    $argNewName: String
                ) {
                    ldataCopy(
                        input: {
                            argSrcNode: $argSrcNode
                            argDstParent: $argDstParent
                            argNewName: $argNewName
                        }
                    ) {
                        clientMutationId
                    }
                }"""),
                {
                    "argSrcNode": src_data.id,
                    "argDstParent": dst_data.id,
                    "argNewName": new_name,
                },
            )
        except TransportQueryError as e:
            if e.errors is None or len(e.errors) == 0:
                raise e

            msg: str = e.errors[0]["message"]

            if msg.startswith("Permission denied on node"):
                node_id = msg.rsplit(" ", 1)[1]
                path = path_by_id[node_id]

                raise ValueError(get_path_error(path, "permission denied.", acc_id))
            elif msg == "Refusing to make node its own parent":
                raise ValueError(
                    get_path_error(dst, f"is a parent of {self.path}.", acc_id)
                )
            elif msg == "Refusing to parent node to an object node":
                raise ValueError(get_path_error(dst, f"object exists at path.", acc_id))
            elif msg == "Refusing to move a share link (or into a share link)":
                raise ValueError(
                    get_path_error(
                        self.path if src_data.type is LDataNodeType.link else dst,
                        f"is a share link.",
                        acc_id,
                    )
                )
            elif msg.startswith("Refusing to copy account root"):
                raise ValueError(
                    get_path_error(self.path, "is an account root.", acc_id)
                )
            elif msg.startswith("Refusing to copy removed node"):
                raise ValueError(get_path_error(self.path, "not found.", acc_id))
            elif msg.startswith("Refusing to copy already in-transit node"):
                raise ValueError(
                    get_path_error(self.path, "copy already in progress.", acc_id)
                )
            elif msg == "Conflicting object in destination":
                raise ValueError(get_path_error(dst, "object exists at path.", acc_id))

            raise ValueError(get_path_error(self.path, str(e), acc_id))

    def download(self, dst: Optional[Union[Path, io.IOBase]]) -> Optional[Path]:
        # todo: perform different actions depending on dst type
        return download(
            self.path,
            dst,
        )

    def read_bytes(self) -> bytes:
        # todo: implement
        pass

    def read_text(self) -> str:
        # todo: implement
        pass

    def read_json(self) -> JsonValue:
        # todo: implement
        pass

    def read_chunks(self, chunk_size: int) -> Generator[bytes, None, None]:
        # todo: implement
        pass

    def read_lines(self):
        # todo: implement
        pass

    def read_at(self, offset: int, amount: int) -> bytes:
        # todo: implement
        pass

    def upload(self, src: Union[Path, io.IOBase, bytes, JsonValue]) -> str:
        # todo: implement
        pass

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


if __name__ == "__main__":
    # add tests here
    pass
