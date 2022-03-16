"""
config.latch
~~~~~
Platform wide configuration, eg. api endpoints, callback server ports...
"""

DEV = "dev"
STAGING = "staging"
PROD = "prod"

CONSOLE_URLS = {
    "dev": "https://console.ligma.ai",
    "staging": "https://console.sugma.ai",
    "prod": "https://console.latch.bio",
}

NUCLEUS_URLS = {
    "dev": "https://nucleus.ligma.ai",
    "staging": "https://nucleus.sugma.ai",
    "prod": "https://nucleus.latch.bio",
}

SDK_ENDPOINTS = {
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
}

ENV = PROD


class LatchConfig:

    dkr_repo = "812206152185.dkr.ecr.us-west-2.amazonaws.com"

    def __init__(self, environment=PROD):
        self.set_environment(environment=environment)

    def set_environment(self, environment):
        self.environment = environment
        self.console_url = CONSOLE_URLS[ENV]
        self.nucleus_url = NUCLEUS_URLS[ENV]
        self.sdk_endpoints = {
            key: f"{self.nucleus_url}{endpoint}"
            for key, endpoint in SDK_ENDPOINTS.items()
        }
