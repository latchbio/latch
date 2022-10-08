"""
config.latch
~~~~~~~~~~~~
Platform wide configuration, eg. api endpoints, callback server ports...
"""

import os

DOMAIN = os.environ.get("LATCH_SDK_DOMAIN", "latch.bio")
CONSOLE_URL = f"https://console.{DOMAIN}"
NUCLEUS_URL = f"https://nucleus.{DOMAIN}"


SDK_ENDPOINTS = {
    "get-test-data-creds": "/sdk/get-test-data-creds",
    "initiate-multipart-upload": "/sdk/initiate-multipart-upload",
    "complete-multipart-upload": "/sdk/complete-multipart-upload",
    "download": "/sdk/download",
    "list-files": "/sdk/list",
    "initiate-image-upload": "/sdk/initiate-image-upload",
    "register-workflow": "/sdk/register-workflow",
    "get-workflow-interface": "/sdk/wf-interface",
    "access-jwt": "/sdk/access-jwt",
    "execute-workflow": "/sdk/wf",
    "get-workflows": "/sdk/get-wf",
    "verify": "/sdk/verify",
    "remove": "/sdk/rm",
    "id": "/sdk/node-id",
    "mkdir": "/sdk/mkdir",
    "rmdir": "/sdk/rmdir",
    "touch": "/sdk/touch",
    "pod-exec-info": "/sdk/get-pod-exec-info",
    "provision-centromere": "/sdk/provision-centromere",
    "preview": "/sdk/workflow-ui-preview",
    "get-ws": "/sdk/get-ws",
    "get-executions": "/sdk/get-executions",
    "get-workflow-graph": "/sdk/get-workflow-graph",
    "get-logs": "/sdk/get-logs-for-node",
    "abort-execution": "/sdk/abort-execution",
    "local-development": "/sdk/initiate-local-development-session",
    "close-local-development": "/sdk/close-local-development-session",
}


class LatchConfig:

    dkr_repo = "812206152185.dkr.ecr.us-west-2.amazonaws.com"

    def __init__(self):
        self.console_url = CONSOLE_URL
        self.nucleus_url = NUCLEUS_URL
        self.sdk_endpoints = {
            key: f"{self.nucleus_url}{endpoint}"
            for key, endpoint in SDK_ENDPOINTS.items()
        }
