# Conditional Sections

In order to support the functionality of an `if-elif-else` clause within the body of a workflow, we introduce the method `create_conditional_section`.

This method creates a new conditional section in a workflow, allowing a user to conditionally execute a task based on the value of a task result.

Conditional sections are akin to ternary operators -- they return the output of the branch result. However, they can be n-ary with as many _elif_ clauses as desired.

It is possible to consume the outputs from conditional nodes. And to pass in outputs from other tasks to conditional nodes.

The boolean expressions in the condition use `&` and `|` as and / or operators. Additionally, binary expressions are not allowed. Thus if a task returns a boolean and we wish to use it in a condition of a conditional block, we must use built in truth checks: `result.is_true()` or `result.is_false()`

```python
from latch import small_task
from latch import create_conditional_section

@small_task
def square(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task is derived from the name of the input variable, and
               the type is automatically mapped to Types.Integer
    Return:
        float: The label for the output is automatically assigned and the type is deduced from the annotation
    """
    return n * n


@small_task
def double(n: float) -> float:
    """
    Parameters:
        n (float): name of the parameter for the task is derived from the name of the input variable
               and the type is mapped to ``Types.Integer``
    Return:
        float: The label for the output is auto-assigned and the type is deduced from the annotation
    """
    return 2 * n

@workflow
def multiplier(my_input: float) -> float:
    result_1 = double(n=my_input)
    result_2 =  (
        create_conditional_section("fractions")
        .if_((result_1 < 0.0)).then(double(n=result_1))
        .elif_((result_1 > 0.0)).then(square(n=result_1))
        .else_().fail("Only nonzero values allowed")
    )
    result_3 = double(n=result_2)
    return result_3
```
