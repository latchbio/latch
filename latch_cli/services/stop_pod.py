from urllib.parse import urljoin

import click
from latch_sdk_config.latch import NUCLEUS_URL

from .. import tinyrequests
from .cp.utils import get_auth_header


def stop_pod(pod_id: int) -> None:
    """Stops a pod given a pod_id"""
    print(urljoin(NUCLEUS_URL, "/pods/stop"))
    res = tinyrequests.post(
        urljoin(NUCLEUS_URL, "/pods/stop"),
        headers={"Authorization": get_auth_header()},
        json={"pod_id": pod_id},
    )

    if res.status_code == 200:
        click.secho(f"Pod `{pod_id}` Stopped.", fg="green")
        return

    if res.status_code == 403:
        click.secho(f"Permission denied for pod `{pod_id}`.", fg="red")
        return

    if res.status_code == 404:
        click.secho(f"Pod `{pod_id}` does not exist.", fg="red")
        return

    if res.status_code == 500:
        click.secho(
            f"Internal error while stopping pod `{pod_id}`. Please try again and"
            " contact `support@latch.bio` if persists.",
            fg="red",
        )
        return

    raise ValueError(f"failed to stop pod `{pod_id}` with code {res.status_code}")
