# Type Converter Examples

The new custom type converter allows you to launch workflows without requiring exact type matches. This means you can pass dataclasses, enums, and other types that are structurally compatible, even if they're not imported from the exact same module as the workflow definition.

## Key Benefits

1. **No exact type import required** - Pass dict instead of dataclass, or a different enum class with the same values
2. **Structural matching** - Types are matched by structure, not identity
3. **Best-effort conversion** - The converter tries to intelligently convert values to the expected types
4. **Better error messages** - Clear messages about what went wrong during conversion

## Examples

### Before (Requires Exact Types)

```python
# This would FAIL if MyDataClass is not imported from the exact same module
from some.workflow.module import MyDataClass

launch(
    wf_name="my_workflow",
    params={
        "input_data": MyDataClass(field1="value1", field2=42)
    }
)
```

### After (Flexible Conversion)

```python
# Option 1: Pass a dict instead
launch(
    wf_name="my_workflow",
    params={
        "input_data": {"field1": "value1", "field2": 42}
    }
)

# Option 2: Use a different dataclass with the same structure
from dataclasses import dataclass

@dataclass
class MyLocalClass:
    field1: str
    field2: int

launch(
    wf_name="my_workflow",
    params={
        "input_data": MyLocalClass(field1="value1", field2=42)
    }
)

# Option 3: Use any object with the right attributes
class SimpleObject:
    def __init__(self):
        self.field1 = "value1"
        self.field2 = 42

launch(
    wf_name="my_workflow",
    params={
        "input_data": SimpleObject()
    }
)
```

### Enum Conversion

```python
# Before: Required exact enum import
from some.workflow.module import Status

# After: Can pass string or int value directly
launch(
    wf_name="my_workflow",
    params={
        "status": "ACTIVE",  # String value
        # or
        "status": 1,  # Integer value
    }
)

# Or use a different enum with the same values
from enum import Enum

class MyStatus(Enum):
    ACTIVE = "ACTIVE"
    INACTIVE = "INACTIVE"

launch(
    wf_name="my_workflow",
    params={
        "status": MyStatus.ACTIVE
    }
)
```

### Nested Structures

```python
# Complex nested structures work too
launch(
    wf_name="my_workflow",
    params={
        "config": {
            "database": {
                "host": "localhost",
                "port": 5432,
                "options": ["ssl", "auth"]
            },
            "retries": 3,
            "enabled": True
        }
    }
)
```

### Collections

```python
# Lists of dataclasses/dicts
launch(
    wf_name="my_workflow",
    params={
        "samples": [
            {"name": "sample1", "value": 1.0},
            {"name": "sample2", "value": 2.0},
            {"name": "sample3", "value": 3.0},
        ]
    }
)
```

### Optional Types

```python
# None values work for optional parameters
launch(
    wf_name="my_workflow",
    params={
        "optional_param": None,
        "required_param": "value"
    }
)
```

## Supported Types

The converter handles all Flyte types:

- **Primitives**: int, float, str, bool, datetime, timedelta
- **Structs**: dataclasses, dicts, objects with attributes
- **Enums**: Enum classes, or raw string/int values
- **Collections**: list, tuple
- **Maps**: dict
- **Blobs**: LatchFile, LatchDir, LPath, or string paths
- **Unions**: Optional types and union types
- **None**: None values

## How It Works

1. The converter inspects the **Flyte LiteralType** from the workflow interface
2. It examines the **structure** of the value you provide
3. It performs **best-effort conversion** to match the expected type
4. For dataclasses/structs, it extracts fields from dicts, dataclasses, or any object with attributes
5. For enums, it accepts Enum instances, strings, or integers
6. For nested structures, it recursively converts each level

## Error Handling

If conversion fails, you'll get a clear error message:

```python
ValueError: Failed to convert parameter 'my_param' with value {...}: 
Cannot convert <class 'int'> to struct
```

This tells you exactly which parameter failed and why.
