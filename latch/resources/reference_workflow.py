from flytekit.core.launch_plan import reference_launch_plan

from latch_cli.utils import current_workspace


def workflow_reference(
    name: str,
    version: str,
):
    return reference_launch_plan(
        project=current_workspace(),
        domain="development",
        name=name,
        version=version,
    )
