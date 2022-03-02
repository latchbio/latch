"""Latch tasks are decorators to turn functions into workflow 'nodes'.

Each task is containerized, versioned and registered with `Flyte`_ when a
workflow is uploaded to Latch. Containerized tasks are then executed on
arbitrary instances as `Kubernetes Pods`_, scheduled using `flytepropeller`_.

The type of instance that the task executes on (eg. number of available
resources, presence of GPU) can be controlled by invoking one of the set of
exported decorators.


..
    from latch import medium_task

    @medium_task
    def my_task(a: int) -> str:
        ...

.. _Kubernetes Pods:
    https://kubernetes.io/docs/concepts/workloads/pods/
.. _flytepropeller:
    https://github.com/flyteorg/flytepropeller
.. _Flyte:
    https://docs.flyte.org/en/latest/
"""

from flytekit import task
from flytekitplugins.pod import Pod
from kubernetes.client.models import (V1Container, V1PodSpec,
                                      V1ResourceRequirements, V1Toleration)


def _get_large_gpu_pod() -> Pod:
    """g5.8xlarge,g5.16xlarge on-demand"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "31", "memory": "120Gi", "nvidia.com/gpu": "1"},
        limits={"cpu": "64", "memory": "256Gi", "nvidia.com/gpu": "1"},
    )
    primary_container.resources = resources

    return Pod(
        pod_spec=V1PodSpec(
            containers=[primary_container],
            tolerations=[V1Toleration(effect="NoSchedule", key="ng", value="gpu-big")],
        ),
        primary_container_name="primary",
    )


def _get_small_gpu_pod() -> Pod:
    """g4dn.2xlarge on-demand"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "7", "memory": "30Gi", "nvidia.com/gpu": "1"},
        limits={"cpu": "8", "memory": "32Gi", "nvidia.com/gpu": "1"},
    )
    primary_container.resources = resources

    return Pod(
        pod_spec=V1PodSpec(
            containers=[primary_container],
            tolerations=[
                V1Toleration(effect="NoSchedule", key="ng", value="gpu-small")
            ],
        ),
        primary_container_name="primary",
    )


def _get_large_pod() -> Pod:
    """md5n.12xlarge on-demand"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "46", "memory": "170"},
        limits={"cpu": "48", "memory": "192"},
    )
    primary_container.resources = resources

    return Pod(
        pod_spec=V1PodSpec(
            containers=[primary_container],
            tolerations=[V1Toleration(effect="NoSchedule", key="ng", value="big")],
        ),
        primary_container_name="primary",
    )


def _get_medium_pod() -> Pod:
    """Returns a pod which will be scheduled on a node with at least 8 cpus and 32 gigs of memory"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "8", "memory": "32Gi"},
        limits={"cpu": "12", "memory": "64Gi"},
    )
    primary_container.resources = resources

    return Pod(
        pod_spec=V1PodSpec(
            containers=[primary_container],
            tolerations=[
                V1Toleration(effect="NoSchedule", key="ng", value="on-demand-medium")
            ],
        ),
        primary_container_name="primary",
    )


def _get_small_pod() -> Pod:
    """any available instance"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "2", "memory": "4Gi"},
        limits={"cpu": "4", "memory": "8Gi"},
    )
    primary_container.resources = resources

    return Pod(
        pod_spec=V1PodSpec(
            containers=[primary_container],
        ),
        primary_container_name="primary",
    )


large_gpu_task = task(task_config=_get_large_gpu_pod())
"""This task will get scheduled on a large GPU-enabled node.

This node is not necessarily dedicated to the task, but the node itself will be
on-demand.

.. list-table:: Title
   :widths: 20 20 20 20 20
   :header-rows: 1

   * - Type
     - CPU
     - RAM
     - GPU
     - On-Demand
   * - Request
     - 31
     - 120Gi
     - 1
     - True
   * - Limit
     - 64
     - 256Gi
     - 1
     - True
"""

small_gpu_task = task(task_config=_get_small_gpu_pod())
"""This task will get scheduled on a small GPU-enabled node.

This node will be dedicated to the task. No other tasks will be allowed to run
on it.

.. list-table:: Title
   :widths: 20 20 20 20 20
   :header-rows: 1

   * - Type
     - CPU
     - RAM
     - GPU
     - On-Demand
   * - Request
     - 7
     - 30Gi
     - 1
     - True
   * - Limit
     - 8
     - 32Gi
     - 1
     - True
"""

large_task = task(task_config=_get_large_pod())
"""This task will get scheduled on a large node.

This node will be dedicated to the task. No other tasks will be allowed to run
on it.

.. list-table:: Title
   :widths: 20 20 20 20 20
   :header-rows: 1

   * - Type
     - CPU
     - RAM
     - GPU
     - On-Demand
   * - Request
     - 46
     - 176Gi
     - 0
     - True
   * - Limit
     - 48
     - 196Gi
     - 0
     - True
"""

small_task = task(task_config=_get_small_pod())
"""This task will get scheduled on a small node.

.. list-table:: Title
   :widths: 20 20 20 20 20
   :header-rows: 1

   * - Type
     - CPU
     - RAM
     - GPU
     - On-Demand
   * - Request
     - 2
     - 4Gi
     - 0
     - False
   * - Limit
     - 4
     - 8Gi
     - 0
     - False
"""
