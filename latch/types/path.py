import io
from pathlib import Path
from typing import Optional, Union

from latch.types.json import JsonValue


class LPath:
    def __init__(self, path: Path):
        self.path = path

    def download(self, dst: Optional[Union[Path, io.IOBase]]) -> Optional[Path]:
        pass

    def upload(self, src: Union[Path, bytes, JsonValue, io.IOBase]) -> None:
        pass
