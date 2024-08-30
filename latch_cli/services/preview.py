import webbrowser
from pathlib import Path

from flytekit.core.workflow import PythonFunctionWorkflow
from google.protobuf.json_format import MessageToJson
from latch_sdk_config.latch import config

import latch_cli.menus as menus
from latch.utils import current_workspace, retrieve_or_login
from latch_cli.centromere.utils import _import_flyte_objects
from latch_cli.tinyrequests import post


# TODO(ayush): make this import the `wf` directory and use the package root
# instead of the workflow name. also redo the frontend, also make it open the
# page
def preview(pkg_root: Path):
    """Generate a preview of the parameter interface for a workflow.

    This will allow a user to see how their parameter interface will look
    without having to first register their workflow.

    Args:
        pkg_root: A valid path pointing to the worklow code a user wishes to
            preview. The path can be absolute or relative.

    Example:
        >>> preview("wf.__init__.alphafold_wf")
    """

    try:
        modules = _import_flyte_objects([pkg_root.resolve()])
        wfs: dict[str, PythonFunctionWorkflow] = {}
        for module in modules:
            for flyte_obj in module.__dict__.values():
                if isinstance(flyte_obj, PythonFunctionWorkflow):
                    wfs[flyte_obj.name] = flyte_obj
        if len(wfs) == 0:
            raise ValueError(f"Unable to find a workflow definition in {pkg_root}")
    except ImportError as e:
        raise ValueError(
            f"Unable to find {e.name} - make sure that all necessary packages"
            " are installed and you have the correct function name."
        )

    wf = list(wfs.values())[0]
    if len(wfs) > 1:
        wf = menus.select_tui(
            title="Select which workflow to preview",
            options=[
                menus.SelectOption(display_name=k, value=v) for k, v in wfs.items()
            ],
        )

    if wf is None:
        return

    resp = post(
        url=config.api.workflow.preview,
        headers={"Authorization": f"Bearer {retrieve_or_login()}"},
        json={
            "workflow_ui_preview": MessageToJson(wf.interface.to_flyte_idl().inputs),
            "ws_account_id": current_workspace(),
        },
    )

    resp.raise_for_status()

    url = f"{config.console_url}/preview/parameters"
    webbrowser.open(url)
