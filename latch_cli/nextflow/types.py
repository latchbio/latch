from enum import Enum


class NextflowProcessExecutor(Enum):
    local = "local"
    k8s = "k8s"
