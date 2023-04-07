from typing import Dict, List, Union

from typing_extensions import TypeAlias

JsonArray: TypeAlias = List["JsonValue"]
JsonObject: TypeAlias = Dict[str, "JsonValue"]
JsonValue: TypeAlias = Union[JsonObject, JsonArray, str, int, float, bool, None]
