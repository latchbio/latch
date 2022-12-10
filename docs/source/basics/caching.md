# Caching

Caching allows workflow developers to cache the results of tasks to prevent
wasted computation.

This is helpful when running large batches of workflows with redundant inputs
or when debugging errors in the middle of a workflow where upstream state can
be "saved".

---

```python
import time
from latch import small_task

@small_task(cache=True)
def do_sleep(foo: str) -> str:

    time.sleep(60)
    return foo
```

You can also pass an optional `cach_version` keyword argument to version your
cache with greater control. Tasks caches with explicit versions will get
invalidated if and only if the version changes. This is ideal if one wishes to
preserve the cache despite the function body changing or to manually invalidate
the cache despite the function body remaining the same.

```python
import time
from latch import small_task

@small_task(cache=True, cache_version="0.0.0")
def do_sleep_with_version(foo: str) -> str:

    time.sleep(60)
    return foo
```

## Task Invalidation Behavior

Each workflow task maintains its own cache that is independent from the version
of its parent workflow. This allows tasks to preserve their cache across workflow
re-registers if other tasks are modified.

A task's cache will get invalidated when:

* code in the task function body (that is not a comment) is changed
* the task function signature (name or typing of input or output parameters) is
  changed
* an (optional) cache version is changed

A task's cache will not get invalidated if the task function body does not
change (comments do not count as changes that invalidate the cache).

## Task Isolation Criteria

A more comprehensive list of what defines a unique cache follows. Between any
two task caches, if any of the following things change, the caches will be
distinct:

* the account to which the task is registered, including:
    * individual user accounts
    * workspaces owned by the same user
* the name of the task (name of the function)
* the function signature of the task (name and typing of all input / output
  parameters)
* the (optional) cache version
