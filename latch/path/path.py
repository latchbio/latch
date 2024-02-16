import io
from enum import Enum
from pathlib import Path
from typing import Generator, Optional, Union

from gql import gql
from latch_sdk_gql.execute import execute

from latch.types.json import JsonValue
from latch_cli.services.cp.ldata_utils import LDataNodeType, get_node_data
from latch_cli.utils.path import is_remote_path


class LPath:
    def __init__(self, path: str):
        if not is_remote_path(path):
            raise ValueError(f"Invalid LPath: {path} is not a Latch path")
        self._path = path

    @property
    def node_id(self) -> str:
        # todo: currently raises click exception which is wrong
        # todo: this function should be moved to this directory
        node_data = get_node_data(self._path).data[self._path]
        return node_data.id

    @property
    def exists(self) -> bool:
        try:
            node_data = get_node_data(self._path).data[self._path]
        except Exception:  # todo: this should be a specific exception
            return False
        return not node_data.removed

    @property
    def type(self) -> LDataNodeType:
        node_data = get_node_data(self._path).data[self._path]
        return node_data.type

    def _fetch_metadata(self):
        data = execute(
            gql("""
            query NodeMetadataQuery($id: BigInt!) {
                ldataNode(id: $id) {
                    ldataObjectMeta {
                        contentSize
                        contentType
                    }
                }
            }
            """),
            variables={"id": self.node_id},
        )["ldataNode"]
        if data is None:
            raise FileNotFoundError(f"{self._path} not found")

        assert "ldataObjectMeta" in data
        return data["ldataObjectMeta"]

    @property
    def size(self) -> float:
        metadata = self._fetch_metadata()
        assert "contentSize" in metadata
        return metadata["contentSize"]

    @property
    def content_type(self) -> str:
        metadata = self._fetch_metadata()
        assert "contentType" in metadata
        return metadata["contentType"]

    def iterdir(self) -> Generator[Path, None, None]:
        pass

    def download(self, dst: Optional[Union[Path, io.IOBase]]) -> Optional[Path]:
        pass

    def upload(self, src: Union[Path, io.IOBase, bytes, JsonValue]) -> None:
        pass
