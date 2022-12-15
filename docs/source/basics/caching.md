# Caching

You can cache the results of tasks to prevent wasted computation. Tasks that are
identified as "the same" (rigorous criteria to identify cache validity to
follow) can succeed instantly with outputs that were saved from previous
executions.

Caching is very helpful when debugging, sharing tasks between workflows, or
batched executions over similar input data.

Every latch task, eg. `small_task`, `large_gpu_task`, has a cachable
equivalent prefixed with cached, eg. `cached_small_task`.

```python
import time
from latch.resources.tasks import cached_small_task

@cached_small_task("someversion")
def do_sleep(foo: str) -> str:

    time.sleep(60)
    return foo
```

## When will my tasks get cached?

Tasks are not cached by default as task code has the option of performing side
effects (eg. communicating with servers or uploading files) that would be
destroyed by caching.

Tasks will only receive a cache when using the `cached_<>` task decorators.

## Task cache isolation

Task caches are not shared between accounts or even necessarily between
workflows within the same account. The following is a list of cases where task
caches are guaranteed to be different.

- each latch account will have its own task cache
- each task with a unique name will have its own task cache
- whenever the task function signature changes (name or typing of input or output parameters) the task will receive a
  new cache
- whenever the task cache version changes, the task will receive a new cache

## Why there is a cache version

We require a cache version because we cannot naively assume that the checksum of
a task function's body should be used in our cache key.

This assumes that changing the task body logic will always change the outputs of
the task. There are many changes we can make to the body (eg. print statements)
that will have no effect on the outputs and would cause an expensive cache
invalidate for no reason.

Therefore we are rolling out the initial cached task feature with user specified
cache versions. If this becomes too cumbersome and the vast majority of
re-registers would benefit from an automatic cache invalidate when task body
code changes, we will pin the cache version with a digest of the function body.
