# Type Converter Implementation

## Overview

The new type converter system allows flexible workflow launching without requiring exact Python type matches. This solves the problem where dataclasses, enums, and other types must be imported from the exact same module as the workflow definition.

## What Was Changed

### 1. New Module: `type_converter.py`

Created a comprehensive type conversion system that:
- Converts Python values to Flyte Literals based on **structure** rather than **type identity**
- Handles all Flyte types with best-effort conversion
- Provides clear error messages when conversion fails

### 2. Updated: `launch_v2.py`

Modified the `launch()` function to use the new converter:
- Replaced `translate_inputs_to_literals` (from flytekit) with `convert_inputs_to_literals` (custom)
- Removed dependency on exact Python type matches
- Simplified error handling

## Key Features

### Structural Type Matching

The converter examines the **Flyte LiteralType** from the workflow interface and converts values based on their structure:

```python
# All of these work for a dataclass parameter:
launch(wf_name="wf", params={"data": {"field": "value"}})  # dict
launch(wf_name="wf", params={"data": MyDataClass(field="value")})  # dataclass
launch(wf_name="wf", params={"data": some_object})  # any object with .field attribute
```

### Enum Flexibility

Enums can be passed as:
- Enum instances (any enum with matching values)
- Raw string values
- Raw integer values

```python
# All equivalent:
launch(wf_name="wf", params={"status": MyEnum.ACTIVE})
launch(wf_name="wf", params={"status": "ACTIVE"})
```

### Type Coverage

The converter handles:

| Type | Support | Notes |
|------|---------|-------|
| Primitives (int, float, str, bool) | ✅ Full | Direct conversion |
| Datetime/Duration | ✅ Full | Accepts datetime objects or ISO strings |
| Structs/Dataclasses | ✅ Full | Accepts dict, dataclass, or any object |
| Enums | ✅ Full | Accepts enum instance or raw value |
| Collections (list) | ✅ Full | Recursive conversion of elements |
| Maps (dict) | ✅ Full | Recursive conversion of values |
| Unions/Optional | ✅ Full | Tries each variant in order |
| Blobs (files) | ✅ Full | Falls back to TypeEngine for files |
| None | ✅ Full | Proper None literal creation |

## Implementation Details

### Conversion Flow

```
User params → convert_inputs_to_literals()
              ↓
              For each parameter:
              ↓
              convert_python_value_to_literal()
              ↓
              Examine Flyte LiteralType:
              ↓
              ├─ Union? → Try each variant
              ├─ Primitive? → Convert to Flyte primitive
              ├─ Struct? → Extract fields, convert recursively
              ├─ Collection? → Convert each element
              ├─ Map? → Convert each value
              └─ Blob? → Use TypeEngine (files are special)
              ↓
              Return Flyte Literal
```

### Key Functions

#### `convert_python_value_to_literal(value, flyte_literal_type, ctx)`
Main conversion function that dispatches to specific handlers based on the Flyte type.

#### `_convert_to_struct(value)`
Converts dicts, dataclasses, or any object with attributes to Flyte struct literals. Uses protobuf's `Struct` type.

#### `_python_value_to_struct_value(value)`
Recursive helper for converting Python values to protobuf `Value` objects within structs.

#### `_convert_union(value, union_type, ctx)`
Handles Union/Optional types by trying each variant until one succeeds.

#### `convert_inputs_to_literals(ctx, params, flyte_interface_types)`
Top-level function that converts all workflow parameters. This replaces `translate_inputs_to_literals`.

## Usage Example

### Before (Exact Types Required)

```python
# workflow.py (registered to Latch)
from dataclasses import dataclass
from enum import Enum

@dataclass
class SampleData:
    name: str
    count: int

class Status(Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"

@workflow
def my_workflow(data: SampleData, status: Status):
    ...

# launch_script.py
# HAD to import exact same classes:
from workflow import SampleData, Status

launch(
    wf_name="my_workflow",
    params={
        "data": SampleData(name="test", count=5),
        "status": Status.ACTIVE
    }
)
```

### After (Flexible Types)

```python
# launch_script.py
# No imports needed! Just pass compatible values:

launch(
    wf_name="my_workflow",
    params={
        "data": {"name": "test", "count": 5},  # dict works
        "status": "active"  # string works
    }
)
```

## Error Handling

The converter provides clear error messages:

```python
ValueError: Failed to convert parameter 'my_param' with value {...}: 
Expected list or tuple, got <class 'str'>
```

This tells you:
1. Which parameter failed
2. What value was provided
3. What went wrong

## Testing

Comprehensive tests are provided in `tests/test_type_converter.py`:
- Primitive conversion tests
- Struct/dataclass tests
- Enum conversion tests
- Collection tests
- Map tests
- Union/Optional tests
- Complex real-world scenarios

Run tests:
```bash
pytest tests/test_type_converter.py -v
```

## Backward Compatibility

The changes are **backward compatible**:
- Old code that passes exact types still works
- New code can use flexible types
- No breaking changes to the API

## Performance

The converter is efficient:
- Single pass through the data structure
- No unnecessary type checking
- Lazy evaluation where possible
- Minimal overhead compared to previous implementation

## Limitations

1. **File types** still use TypeEngine as a fallback (they work, just use the old system)
2. **Type validation** is best-effort (structural, not strict)
3. **Metadata loss** - some type metadata may be lost in conversion

## Future Improvements

Potential enhancements:
1. Add caching for repeated conversions
2. Support custom type transformers
3. Add validation hooks
4. Improve error messages with suggestions
5. Support more Flyte types as they're added

## Migration Guide

### If you're currently using `launch()`:

No changes needed! Your code will continue to work. To take advantage of flexible types:

1. **Remove unnecessary imports** of workflow types
2. **Pass dicts instead of dataclasses** if convenient
3. **Pass raw enum values** instead of enum instances
4. **Simplify test data** by using simple Python types

### If you're using `launch_from_launch_plan()`:

No changes to this function yet. It still works as before. The converter is only used in the `launch()` function.
