# Caching

Caching allows workflow developers to reuse the results of previously run tasks
to prevent wasted time and computation.

This is helpful when running large batches of workflows with redundant inputs
or when debugging errors in the middle of a workflow where upstream state can
be reused.

---

```python
import time
from latch import small_task

@small_task(cache=True)
def do_sleep(foo: str) -> str:

    time.sleep(60)
    return foo
```

You can also pass the optional `cache_version` keyword argument to version your
cache giving you greater control. Tasks caches with explicit versions will get
invalidated if and only if the version changes. This is ideal if you wish to
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

## Caching behavior with tasks

Each task maintains its own cache that is independent from whatever workflow it
happens to be associated with.  This allows tasks to preserve their cache
across workflow re-registers if other tasks are modified.

Examples of when a task's cache will get invalidated:

* code in the task function body (that is not a comment) is changed
* the task function signature (name or typing of input or output parameters) is
  changed
* an (optional) cache version is changed

Examples of when a task's cache will remain unchanged:

* the task function body does not
    change (comments do not count as changes that invalidate the cache).
* a new workflow was created with a task of the same name, signature and body
  (remember that task caches are independent from workflows that contain them)

## When does my cache get invalidated?

A task's cache will be invalidated and the task will be run from scratch if any
of the following change between executions:

* the account to which the task is registered, including:
  * individual user accounts
  * workspaces owned by the same user
* the name of the task (name of the function)
* the function signature of the task (name and typing of all input / output
  parameters)
* the (optional) cache version
