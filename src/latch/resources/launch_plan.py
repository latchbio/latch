from __future__ import annotations

from typing import Any

from flytekit.core.launch_plan import LaunchPlan as _LaunchPlan
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.models.common import Labels


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

    def __init__(
        self,
        workflow: PythonFunctionWorkflow,
        name: str,
        default_params: dict[str, Any],
        description: str | None = None,
    ):
        try:  # noqa: SIM105
            _LaunchPlan.create(
                f"{workflow.__module__}.{workflow.__name__}.{name}",
                workflow,
                default_params,
                labels=Labels({"description": description}),
            )

        # if the launchplan already exists, the `create` method throws an AssertionError
        except AssertionError:
            pass
