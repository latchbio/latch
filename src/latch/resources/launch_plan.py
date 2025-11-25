from __future__ import annotations

import base64
from typing import Any

from flytekit.core.launch_plan import LaunchPlan as _LaunchPlan
from flytekit.core.workflow import PythonFunctionWorkflow
from flytekit.models.common import Annotations


def b62encode(plain: str) -> str:
    b64 = base64.b64encode(plain.encode()).decode()
    return b64.replace("0", "00").replace("+", "01").replace("/", "02").replace("=", "")


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
        *,
        description: str | None = None,
    ):
        labels = {}
        if description is not None:
            labels["description"] = b62encode(description)

        if "." in name:
            raise ValueError("LaunchPlan name cannot contain the '.' character")

        try:  # noqa: SIM105
            _LaunchPlan.create(
                f"{workflow.__module__}.{workflow.__name__}.{name}",
                workflow,
                default_params,
                annotations=Annotations(labels),
            )

        # if the launchplan already exists, the `create` method throws an AssertionError
        except AssertionError:
            pass
