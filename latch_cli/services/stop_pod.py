from urllib.parse import urljoin

import click
from latch_sdk_config.latch import NUCLEUS_URL

from .. import tinyrequests
from ..utils import get_auth_header


def stop_pod(pod_id: int) -> None:
    """Stops a pod given a pod_id"""
    res = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/pods/stop"),
        headers={"Authorization": get_auth_header()},
        json={"pod_id": pod_id},
    )

    if res.status_code == 200:
        click.secho(f"Pod with ID `{pod_id}` stopped.", fg="green")
        return

    if res.status_code == 403 or res.status_code == 404:
        click.secho("Pod does not exist or permission denied.", fg="red")
        return

    if res.status_code != 200:
        click.secho(
            f"Internal error while stopping Pod `{pod_id}`. Please try again."
            " contact `support@latch.bio` if the issue persists.",
            fg="red",
        )
        return
