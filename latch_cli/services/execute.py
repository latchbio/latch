import json
import os
import select
import sys
import termios
import textwrap
from pathlib import Path
from tty import setraw
from typing import Tuple

import kubernetes
import requests
import websocket
from kubernetes.client.api import core_v1_api
from kubernetes.stream import stream
from latch_sdk_config.latch import config

from latch_cli.utils import account_id_from_token, current_workspace, retrieve_or_login


def _construct_kubeconfig(
    cert_auth_data: str,
    cluster_endpoint: str,
    account_id: str,
    access_key: str,
    secret_key: str,
    session_token: str,
) -> str:
    open_brack = "{"
    close_brack = "}"
    region_code = "us-west-2"
    cluster_name = "prion-prod"

    return textwrap.dedent(f"""apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: {cert_auth_data}
    server: {cluster_endpoint}
  name: arn:aws:eks:{region_code}:{account_id}:cluster/{cluster_name}
contexts:
- context:
    cluster: arn:aws:eks:{region_code}:{account_id}:cluster/{cluster_name}
    user: arn:aws:eks:{region_code}:{account_id}:cluster/{cluster_name}
  name: arn:aws:eks:{region_code}:{account_id}:cluster/{cluster_name}
current-context: arn:aws:eks:{region_code}:{account_id}:cluster/{cluster_name}
kind: Config
preferences: {open_brack}{close_brack}
users:
- name: arn:aws:eks:{region_code}:{account_id}:cluster/{cluster_name}
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      command: aws
      args:
        - --region
        - {region_code}
        - eks
        - get-token
        - --cluster-name
        - {cluster_name}
      env:
        - name: 'AWS_ACCESS_KEY_ID'
          value: '{access_key}'
        - name: 'AWS_SECRET_ACCESS_KEY'
          value: '{secret_key}'
        - name: 'AWS_SESSION_TOKEN'
          value: '{session_token}'""")


def _fetch_pod_info(token: str, task_name: str) -> Tuple[str, str, str]:
    headers = {"Authorization": f"Bearer {token}"}
    data = {"task_name": task_name, "ws_account_id": current_workspace()}

    response = requests.post(config.api.execution.exec, headers=headers, json=data)

    try:
        response = response.json()
        access_key = response["tmp_access_key"]
        secret_key = response["tmp_secret_key"]
        session_token = response["tmp_session_token"]
        cert_auth_data = response["cert_auth_data"]
        cluster_endpoint = response["cluster_endpoint"]
        namespace = response["namespace"]
        aws_account_id = response["aws_account_id"]
    except KeyError as err:
        raise ValueError(f"malformed response: {response}") from err

    return (
        access_key,
        secret_key,
        session_token,
        cert_auth_data,
        cluster_endpoint,
        namespace,
        aws_account_id,
    )


def execute(task_name: str):
    """Allows a user to start an interactive shell session in the remote machine
    that a task is running on.

    When running a workflow on Latch, its often helpful while debugging to have
    a direct way of interacting with the machines on which tasks are run. Using
    `execute`, a user can easily get a shell into the machine on which the
    specified task is running.

    Args:
        task_name: The name of the running task you want a shell into. This is a
            hash that can be found in the sidebar in the browser display of the
            running workflow.

    Example:
        >>> execute("abcd1234-n0")
            root@1.2.3.4:~$

    """

    token = retrieve_or_login()
    (
        access_key,
        secret_key,
        session_token,
        cert_auth_data,
        cluster_endpoint,
        namespace,
        aws_account_id,
    ) = _fetch_pod_info(token, task_name)

    account_id = account_id_from_token(token)
    if int(account_id) < 10:
        account_id = f"x{account_id}"

    config_data = _construct_kubeconfig(
        cert_auth_data,
        cluster_endpoint,
        aws_account_id,
        access_key,
        secret_key,
        session_token,
    )
    config_file = Path("config").resolve()

    with open(config_file, "w") as c:
        c.write(config_data)

    kubernetes.config.load_kube_config("config")

    core_v1 = core_v1_api.CoreV1Api()

    # TODO
    pod_name = task_name

    stdin_channel = bytes([kubernetes.stream.ws_client.STDIN_CHANNEL])
    stdout_channel = kubernetes.stream.ws_client.STDOUT_CHANNEL
    stderr_channel = kubernetes.stream.ws_client.STDERR_CHANNEL

    class WSStream:
        def __init__(self):
            self._wssock = stream(
                core_v1.connect_get_namespaced_pod_exec,
                pod_name,
                namespace,
                command=["/bin/sh"],
                stderr=True,
                stdin=True,
                stdout=True,
                tty=True,
                _preload_content=False,
            ).sock

        def send(self, chunk: bytes):
            self._wssock.send(stdin_channel + chunk, websocket.ABNF.OPCODE_BINARY)

        def get_frame(
            self,
        ) -> Tuple[int, websocket.ABNF]:
            return self._wssock.recv_data_frame(True)

        @property
        def socket(self):
            return self._wssock.sock

        def close(self):
            self._wssock.close()

    class TTY:
        def __init__(
            self,
            in_stream: int,
            out_stream: int,
            err_stream: int,
            raw: bool = True,
        ):
            if raw:
                setraw(sys.stdin.fileno())

            self._stdin = in_stream
            self._stdout = out_stream
            self._stderr = err_stream

        def flush(self) -> bytes:
            return os.read(self._stdin, 32 * 1024)

        def write_out(self, chunk: bytes):
            os.write(self._stdout, chunk)

        def write_err(self, chunk: bytes):
            os.write(self._stderr, chunk)

        @property
        def in_stream(self):
            return self._stdin

    try:
        old_settings = termios.tcgetattr(sys.stdin.fileno())

        tty_ = TTY(
            sys.stdin.fileno(),
            sys.stdout.fileno(),
            sys.stderr.fileno(),
        )
        try:
            wsstream = WSStream()
        except kubernetes.client.rest.ApiException:
            raise ValueError(
                "Unable to find requested task name - make sure that you are in the"
                " correct workspace."
            )

        rlist = [wsstream.socket, tty_.in_stream]
        while True:
            rs, _, _ = select.select(rlist, [], [])

            if tty_.in_stream in rs:
                chunk = tty_.flush()
                if len(chunk) > 0:
                    wsstream.send(chunk)

            if wsstream.socket in rs:
                opcode, frame = wsstream.get_frame()
                if opcode == websocket.ABNF.OPCODE_CLOSE:
                    rlist.remove(wsstream.socket)

                elif opcode == websocket.ABNF.OPCODE_BINARY:
                    channel = frame.data[0]
                    chunk = frame.data[1:]
                    if channel in (stdout_channel, stderr_channel):
                        if len(chunk):
                            if channel == stdout_channel:
                                tty_.write_out(chunk)
                            else:
                                tty_.write_err(chunk)
                    elif channel == kubernetes.stream.ws_client.ERROR_CHANNEL:
                        wsstream.close()
                        error = json.loads(chunk)
                        if error["status"] == "Success":
                            break
                        raise websocket.WebSocketException(
                            f"Status: {error['status']} - Message: {error['message']}"
                        )
                    else:
                        raise websocket.WebSocketException(
                            f"Unexpected channel: {channel}"
                        )

                else:
                    raise websocket.WebSocketException(
                        f"Unexpected websocket opcode: {opcode}"
                    )
    finally:
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSANOW, old_settings)
