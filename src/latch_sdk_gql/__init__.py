class AuthenticationError(RuntimeError):
    ...

from typing import Dict, List, Union

from typing_extensions import TypeAlias

JsonArray: TypeAlias = List["JsonValue"]
"""JSON-compatible list"""

JsonObject: TypeAlias = Dict[str, "JsonValue"]
"""JSON-compatible dictionary"""

JsonValue: TypeAlias = Union[JsonObject, JsonArray, str, int, float, bool, None]
"""JSON-compatible value

Can be a dictionary, an array, or a primitive value
"""
