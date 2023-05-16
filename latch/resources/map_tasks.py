"""
A map task lets you run a pod task or a regular task over a 
list of inputs within a single workflow node. This means you 
can run thousands of instances of the task without creating 
a node for every instance, providing valuable performance 
gains!

Some use cases of map tasks include:
 - Several inputs must run through the same code logic
 - Multiple data batches need to be processed in parallel
 - Hyperparameter optimization

Args:
    task_function: The task to be mapped, to be shown in Latch Console

Returns:
    A conditional section

Intended Use: ::

    @task
    def a_mappable_task(a: int) -> str:
        inc = a + 2
        stringified = str(inc)
        return stringified

    @task
    def coalesce(b: typing.List[str]) -> str:
        coalesced = "".join(b)
        return coalesced

    @workflow
    def my_map_workflow(a: typing.List[int]) -> str:
        mapped_out = map_task(a_mappable_task)(a=a).with_overrides(
            requests=Resources(mem="300Mi"),
            limits=Resources(mem="500Mi"),
            retries=1,
        )
        coalesced = coalesce(b=mapped_out)
        return coalesced
"""

from flytekit.core.map_task import map_task
