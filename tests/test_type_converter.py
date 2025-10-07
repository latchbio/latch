"""Tests for the type converter module."""

from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum

import pytest
from flytekit.core.context_manager import FlyteContextManager
from flytekit.models import literals as _literals
from flytekit.models import types as _types

from latch_cli.services.launch.type_converter import (
    convert_inputs_to_literals,
    convert_python_value_to_literal,
)


class TestPrimitiveConversion:
    """Test conversion of primitive types."""

    def test_integer_conversion(self):
        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.INTEGER)

        result = convert_python_value_to_literal(42, literal_type, ctx)

        assert result.scalar.primitive.integer == 42

    def test_float_conversion(self):
        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.FLOAT)

        result = convert_python_value_to_literal(3.14, literal_type, ctx)

        assert result.scalar.primitive.float_value == 3.14

    def test_string_conversion(self):
        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.STRING)

        result = convert_python_value_to_literal("hello", literal_type, ctx)

        assert result.scalar.primitive.string_value == "hello"

    def test_boolean_conversion(self):
        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.BOOLEAN)

        result = convert_python_value_to_literal(True, literal_type, ctx)

        assert result.scalar.primitive.boolean is True


class TestStructConversion:
    """Test conversion of dataclasses and structs."""

    def test_dict_to_struct(self):
        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.STRUCT)

        data = {"name": "Alice", "age": 30, "active": True}
        result = convert_python_value_to_literal(data, literal_type, ctx)

        assert result.scalar.generic.fields["name"].string_value == "Alice"
        assert result.scalar.generic.fields["age"].number_value == 30
        assert result.scalar.generic.fields["active"].bool_value is True

    def test_dataclass_to_struct(self):
        @dataclass
        class Person:
            name: str
            age: int
            active: bool

        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.STRUCT)

        person = Person(name="Bob", age=25, active=False)
        result = convert_python_value_to_literal(person, literal_type, ctx)

        assert result.scalar.generic.fields["name"].string_value == "Bob"
        assert result.scalar.generic.fields["age"].number_value == 25
        assert result.scalar.generic.fields["active"].bool_value is False

    def test_nested_struct(self):
        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.STRUCT)

        data = {
            "user": {"name": "Charlie", "id": 123},
            "settings": {"theme": "dark", "notifications": True},
        }
        result = convert_python_value_to_literal(data, literal_type, ctx)

        user_struct = result.scalar.generic.fields["user"].struct_value
        assert user_struct.fields["name"].string_value == "Charlie"
        assert user_struct.fields["id"].number_value == 123

        settings_struct = result.scalar.generic.fields["settings"].struct_value
        assert settings_struct.fields["theme"].string_value == "dark"
        assert settings_struct.fields["notifications"].bool_value is True


class TestEnumConversion:
    """Test conversion of enums."""

    def test_string_enum_conversion(self):
        class Status(Enum):
            ACTIVE = "active"
            INACTIVE = "inactive"

        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.STRING)

        result = convert_python_value_to_literal(Status.ACTIVE, literal_type, ctx)

        assert result.scalar.primitive.string_value == "active"

    def test_integer_enum_conversion(self):
        class Priority(Enum):
            LOW = 1
            MEDIUM = 2
            HIGH = 3

        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.INTEGER)

        result = convert_python_value_to_literal(Priority.HIGH, literal_type, ctx)

        assert result.scalar.primitive.integer == 3

    def test_raw_enum_value(self):
        """Test that we can pass the raw value instead of enum instance."""
        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.STRING)

        result = convert_python_value_to_literal("active", literal_type, ctx)

        assert result.scalar.primitive.string_value == "active"


class TestCollectionConversion:
    """Test conversion of lists and collections."""

    def test_list_of_integers(self):
        ctx = FlyteContextManager.current_context()
        element_type = _types.LiteralType(simple=_types.SimpleType.INTEGER)
        literal_type = _types.LiteralType(collection_type=element_type)

        result = convert_python_value_to_literal([1, 2, 3, 4, 5], literal_type, ctx)

        assert len(result.collection.literals) == 5
        assert result.collection.literals[0].scalar.primitive.integer == 1
        assert result.collection.literals[4].scalar.primitive.integer == 5

    def test_list_of_structs(self):
        ctx = FlyteContextManager.current_context()
        element_type = _types.LiteralType(simple=_types.SimpleType.STRUCT)
        literal_type = _types.LiteralType(collection_type=element_type)

        data = [
            {"name": "item1", "value": 10},
            {"name": "item2", "value": 20},
        ]
        result = convert_python_value_to_literal(data, literal_type, ctx)

        assert len(result.collection.literals) == 2
        first_item = result.collection.literals[0].scalar.generic
        assert first_item.fields["name"].string_value == "item1"
        assert first_item.fields["value"].number_value == 10


class TestMapConversion:
    """Test conversion of dictionaries to maps."""

    def test_string_to_int_map(self):
        ctx = FlyteContextManager.current_context()
        value_type = _types.LiteralType(simple=_types.SimpleType.INTEGER)
        literal_type = _types.LiteralType(map_value_type=value_type)

        data = {"a": 1, "b": 2, "c": 3}
        result = convert_python_value_to_literal(data, literal_type, ctx)

        assert len(result.map.literals) == 3
        assert result.map.literals["a"].scalar.primitive.integer == 1
        assert result.map.literals["b"].scalar.primitive.integer == 2
        assert result.map.literals["c"].scalar.primitive.integer == 3


class TestUnionConversion:
    """Test conversion of union/optional types."""

    def test_none_in_union(self):
        ctx = FlyteContextManager.current_context()
        
        # Create a union type: Union[int, None]
        union_type = _types.UnionType(
            variants=[
                _types.LiteralType(simple=_types.SimpleType.INTEGER),
                _types.LiteralType(simple=_types.SimpleType.NONE),
            ]
        )
        literal_type = _types.LiteralType(union_type=union_type)

        result = convert_python_value_to_literal(None, literal_type, ctx)

        assert result.scalar.union.type.simple == _types.SimpleType.NONE

    def test_value_in_union(self):
        ctx = FlyteContextManager.current_context()
        
        # Create a union type: Union[str, int]
        union_type = _types.UnionType(
            variants=[
                _types.LiteralType(simple=_types.SimpleType.STRING),
                _types.LiteralType(simple=_types.SimpleType.INTEGER),
            ]
        )
        literal_type = _types.LiteralType(union_type=union_type)

        # Try with string value
        result = convert_python_value_to_literal("hello", literal_type, ctx)

        assert result.scalar.union.value.scalar.primitive.string_value == "hello"
        assert result.scalar.union.type.simple == _types.SimpleType.STRING


class TestComplexScenarios:
    """Test complex, real-world scenarios."""

    def test_mixed_dataclass_and_dict(self):
        """Test that we can mix dataclasses and dicts in the same structure."""

        @dataclass
        class Config:
            host: str
            port: int

        ctx = FlyteContextManager.current_context()
        
        # List of structs
        element_type = _types.LiteralType(simple=_types.SimpleType.STRUCT)
        literal_type = _types.LiteralType(collection_type=element_type)

        # Mix dataclass instances and dicts
        data = [
            Config(host="localhost", port=8080),
            {"host": "example.com", "port": 443},
        ]
        result = convert_python_value_to_literal(data, literal_type, ctx)

        assert len(result.collection.literals) == 2
        
        first_item = result.collection.literals[0].scalar.generic
        assert first_item.fields["host"].string_value == "localhost"
        assert first_item.fields["port"].number_value == 8080
        
        second_item = result.collection.literals[1].scalar.generic
        assert second_item.fields["host"].string_value == "example.com"
        assert second_item.fields["port"].number_value == 443

    def test_duck_typed_object(self):
        """Test that any object with the right attributes works."""

        class SimpleObject:
            def __init__(self):
                self.name = "test"
                self.count = 42

        ctx = FlyteContextManager.current_context()
        literal_type = _types.LiteralType(simple=_types.SimpleType.STRUCT)

        obj = SimpleObject()
        result = convert_python_value_to_literal(obj, literal_type, ctx)

        assert result.scalar.generic.fields["name"].string_value == "test"
        assert result.scalar.generic.fields["count"].number_value == 42


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
