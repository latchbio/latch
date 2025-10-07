# Quick Start: Flexible Type Converter

## What Changed?

The `launch()` function now accepts **any structurally compatible values** - no more exact type import requirements!

## Quick Examples

### Before (Exact Types Required) ❌
```python
from workflow_module import MyDataClass, MyEnum  # Had to import exact types!

launch(wf_name="wf", params={
    "data": MyDataClass(field="value"),  # Exact class required
    "status": MyEnum.ACTIVE              # Exact enum required
})
```

### After (Flexible Types) ✅
```python
# No imports needed!

launch(wf_name="wf", params={
    "data": {"field": "value"},  # Dict works!
    "status": "ACTIVE"           # String works!
})
```

## Common Use Cases

### 1. Dataclasses → Use Dicts
```python
# Workflow has: data: MyDataClass
# You can pass:
launch(wf_name="wf", params={"data": {"field1": "a", "field2": 42}})
```

### 2. Enums → Use Strings/Ints
```python
# Workflow has: status: StatusEnum
# You can pass:
launch(wf_name="wf", params={"status": "ACTIVE"})  # string value
# or
launch(wf_name="wf", params={"status": 1})  # int value
```

### 3. Lists of Dataclasses → Lists of Dicts
```python
# Workflow has: samples: List[SampleInfo]
# You can pass:
launch(wf_name="wf", params={
    "samples": [
        {"id": "S1", "name": "Sample 1"},
        {"id": "S2", "name": "Sample 2"}
    ]
})
```

### 4. Nested Structures
```python
# Complex nested types work too
launch(wf_name="wf", params={
    "config": {
        "database": {"host": "localhost", "port": 5432},
        "options": ["opt1", "opt2"],
        "enabled": True
    }
})
```

## Key Benefits

✅ **No imports needed** - Just pass compatible values  
✅ **Easier testing** - Use simple dicts instead of complex types  
✅ **Flexible** - Mix dicts, dataclasses, and objects freely  
✅ **Backward compatible** - Old code still works  
✅ **Clear errors** - Know exactly what went wrong  

## Files Created

- `type_converter.py` - Core converter implementation
- `TYPE_CONVERTER_README.md` - Full technical docs
- `CONVERTER_EXAMPLES.md` - Usage examples
- `INTEGRATION_EXAMPLE.py` - End-to-end examples
- `tests/test_type_converter.py` - Comprehensive tests
- `IMPLEMENTATION_SUMMARY.md` - Complete overview

## Need More Info?

- **Examples**: See `CONVERTER_EXAMPLES.md`
- **Technical details**: See `TYPE_CONVERTER_README.md`
- **Full examples**: See `INTEGRATION_EXAMPLE.py`
- **Tests**: Run `pytest tests/test_type_converter.py -v`

## Try It Now!

```python
from latch_cli.services.launch.launch_v2 import launch

# Launch with simple types
execution = launch(
    wf_name="your_workflow",
    params={
        # Use dicts, strings, ints - whatever is easiest!
        "param1": {"field": "value"},
        "param2": "enum_value",
        "param3": [1, 2, 3]
    }
)

print(f"Launched: {execution.id}")
```
