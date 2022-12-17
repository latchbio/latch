# Retries

Retries are useful to automatically re-run a task if it fails.

```python
import time
from latch import small_task

@small_task(cache=True, retries=3)
def do_sleep(foo: str) -> str:

    time.sleep(60)
    return foo
```

The number of desired retries is specified as a keyword argument to the task
decorator (as seen above). Tasks are not retried by default.

The following class of errors are handled by retries:

* Python exceptions that originate from within the function body of a task
* out-of-memory or disk pressure exceptions that are the side effect of task
execution


## Underlying node failures and retries

Task failures due to instance pre-emption or other varieties of node issues
that block an otherwise successful task execution will trigger an automatic
retry. This type of retry will occur automatically and is unrelated to the
task argument documented above.
