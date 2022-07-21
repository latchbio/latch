from latch_cli.config.latch import LatchConfig
from latch_cli.tinyrequests import post
from latch_cli.tui import tui_select, tui_select_table
from latch_cli.utils import retrieve_or_login

config = LatchConfig()
endpoints = config.sdk_endpoints


def get_executions():
    token = retrieve_or_login()
    headers = {"Authorization": f"Bearer {token}"}

    resp = post(url=endpoints["get-executions"], headers=headers)

    resp.raise_for_status()

    options = []
    data = resp.json()
    columns = [
        "Name",
        "Workflow",
        "Date",
        "Status",
    ]

    for id, execution_data in sorted(data.items(), key=lambda x: -int(x[0])):
        options.append(
            {
                "Name": execution_data["display_name"],
                "Workflow": f'{execution_data["workflow_name"]}/{execution_data["workflow_version"]}',
                "Date": execution_data["start_time"],
                "Status": execution_data["status"],
            }
        )

    tui_select_table(
        title="Executions",
        column_names=columns,
        options=options,
        clear_terminal=True,
    )
