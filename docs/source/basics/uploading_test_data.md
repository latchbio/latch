# Uploading Test Data

When registering workflows, it is often desirable to upload sets of test data so
that users can quickly execute the workflow, inspect outputs, and get a feel for
what sample values could look like.

To this aim, the SDK provides the ability to define sets of sample parameter
values and to host example files.

## Defining Test Data

If we have a toy workflow:

```python
@workflow
def foo(a: int, b: string, c: LatchFile):
  ...
```

We can define some test data using a `LaunchPlan` like so:

```python
from latch.resources.launch_plan import LaunchPlan

LaunchPlan(
  foo, # Name of the workflow
  "Some Numbers", # Name of the set of test data
  {"a": 1, "b": "bar", "c": LatchFile("s3://latch-public/test-data/42/foo.txt")} # A mapping of parameter names to values
)
```

A `LaunchPlan` needs:

- The name of the workflow it describes.
- The name to describe the test data set (this will be displayed on the console).
- A mapping of parameter names to parameter values. These values will be filled
  in on the interface when a user selects this set of test data.

One just needs to re-register the workflow with this `LaunchPlan` defined in the
same file and the `LaunchPlan` name will show up under the "Use Test Data"
drop-down on the workflow parameters page.

## Hosting Test Files

Notice that in the above example, our test value for the `c` parameter is a
`LatchFile` that points to some object in S3. To upload your own objects, so
that others can use them in their `LaunchPlan`s, we have provided some utility
subcommands on the CLI.

```console
$ latch test-data upload README.md
Successfully uploaded to s3://latch-public/test-data/4107/README.md

$ latch test-data ls
Listing your managed objects by full S3 path.

        s3://latch-public/test-data/4107/README.md
        s3://latch-public/test-data/4107/traceback.txt

$ latch test-data remove README.md
```

After uploading a file, you can simply refer to it using a
`LatchFile("s3://your_path")` as in the example above.
