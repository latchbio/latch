from dataclasses import dataclass
from typing import Any, Dict, Type


@dataclass(frozen=True)
class Record:
    id: str
    name: str

    _value_dict: Dict[str, Any]

    def get_value(self, key: str):
        if key not in self._value_dict:
            raise KeyError(f"Column {key} not found in record")
        return self._value_dict[key]

    def get_type(self, key):
        if key not in self._type_dict:
            raise KeyError(f"Column {key} not found in record")
        return self._type_dict[key]
