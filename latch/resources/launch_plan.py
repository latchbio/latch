import inspect
from typing import Any, Callable, Dict

from flytekit import LaunchPlan as _LaunchPlan


class LaunchPlan:
    """Construct named groups of default parameters for your workflows.

    Pass a workflow function and a dictionary of parameter names mapped to
    default python values and a set of "test data" will be populated on the
    console upon registration.

    Args:
        workflow: The workflow function to which the values apply.
        name: A semantic identifier to the parameter values (eg. 'Small Data')
        default_params: A mapping of paramter names to values

    ..
        from latch.resources.launch_plan import LaunchPlan

        LaunchPlan(
            assemble_and_sort,
            "foo",
            {"read1": LatchFile("latch:///foobar"), "read2": LatchFile("latch:///foobar")},
        )
    """

    def __init__(self, workflow: Callable, name: str, default_params: Dict[str, Any]):

        # This constructor is invoked twice when task code is executed.
        #   1. When the pyflyte-execute entrypoint is invoked to start task.
        #      `mod.__name__`  of caller is `wf`
        #   2. When the PythonAutoContainer loads our module to call our task.
        #      `mod.__name__`  of caller is `wf.__init__`
        # LaunchPlans are stored in a global array and redundant additions will
        # throw an exception, so we want to create on the first constructor
        # call only.

        frame = inspect.stack()[1]
        mod = inspect.getmodule(frame[0])
        if mod.__name__ == "wf":
            str_repr = f"wf.__init__.{workflow.__name__}.{name}"
            _LaunchPlan.create(str_repr, workflow, default_params)
