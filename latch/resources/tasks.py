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

import functools
from warnings import warn

from flytekit import task
from flytekitplugins.pod import Pod
from kubernetes.client.models import (
    V1Container,
    V1PodSpec,
    V1ResourceRequirements,
    V1Toleration,
)


def _get_large_gpu_pod() -> Pod:
    """g5.8xlarge,g5.16xlarge on-demand"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={
            "cpu": "31",
            "memory": "120Gi",
            "nvidia.com/gpu": "1",
            "ephemeral-storage": "1500Gi",
        },
        limits={
            "cpu": "64",
            "memory": "256Gi",
            "nvidia.com/gpu": "1",
            "ephemeral-storage": "2000Gi",
        },
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
        requests={
            "cpu": "7",
            "memory": "30Gi",
            "nvidia.com/gpu": "1",
            "ephemeral-storage": "1500Gi",
        },
        limits={
            "cpu": "8",
            "memory": "32Gi",
            "nvidia.com/gpu": "1",
            "ephemeral-storage": "2000Gi",
        },
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
    """[ "c6i.24xlarge", "c5.24xlarge", "c5.metal", "c5d.24xlarge", "c5d.metal" ]"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "90", "memory": "170Gi", "ephemeral-storage": "4500Gi"},
        limits={"cpu": "96", "memory": "192Gi", "ephemeral-storage": "5000Gi"},
    )
    primary_container.resources = resources

    return Pod(
        annotations={
            "io.kubernetes.cri-o.userns-mode": (
                "private:uidmapping=0:1048576:65536;gidmapping=0:1048576:65536"
            )
        },
        pod_spec=V1PodSpec(
            runtime_class_name="sysbox-runc",
            containers=[primary_container],
            tolerations=[
                V1Toleration(effect="NoSchedule", key="ng", value="cpu-96-spot")
            ],
        ),
        primary_container_name="primary",
    )


def _get_medium_pod() -> Pod:
    """[ "m5.8xlarge", "m5ad.8xlarge", "m5d.8xlarge", "m5n.8xlarge", "m5dn.8xlarge", "m5a.8xlarge" ]
    """

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "28", "memory": "100Gi", "ephemeral-storage": "1500Gi"},
        limits={"cpu": "32", "memory": "128Gi", "ephemeral-storage": "2000Gi"},
    )
    primary_container.resources = resources

    return Pod(
        annotations={
            "io.kubernetes.cri-o.userns-mode": (
                "private:uidmapping=0:1048576:65536;gidmapping=0:1048576:65536"
            )
        },
        pod_spec=V1PodSpec(
            runtime_class_name="sysbox-runc",
            containers=[primary_container],
            tolerations=[
                V1Toleration(effect="NoSchedule", key="ng", value="cpu-32-spot")
            ],
        ),
        primary_container_name="primary",
    )


def _get_small_pod() -> Pod:
    """any available instance"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "2", "memory": "4Gi", "ephemeral-storage": "100Gi"},
        limits={"cpu": "4", "memory": "8Gi", "ephemeral-storage": "500Gi"},
    )
    primary_container.resources = resources

    return Pod(
        annotations={
            "io.kubernetes.cri-o.userns-mode": (
                "private:uidmapping=0:1048576:65536;gidmapping=0:1048576:65536"
            )
        },
        pod_spec=V1PodSpec(
            runtime_class_name="sysbox-runc",
            containers=[primary_container],
        ),
        primary_container_name="primary",
    )


large_gpu_task = functools.partial(task, task_config=_get_large_gpu_pod())
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


small_gpu_task = functools.partial(task, task_config=_get_small_gpu_pod())
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


large_task = functools.partial(task, task_config=_get_large_pod())
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
     - 90
     - 176Gi
     - 0
     - True
   * - Limit
     - 96
     - 196Gi
     - 0
     - True
"""


medium_task = functools.partial(task, task_config=_get_medium_pod())
"""This task will get scheduled on a medium node.

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
     - 8
     - 32Gi
     - 0
     - True
   * - Limit
     - 12
     - 64Gi
     - 0
     - True
"""


small_task = functools.partial(task, task_config=_get_small_pod())
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


def custom_memory_optimized_task(cpu: int, memory: int):
    """Returns a custom task configuration requesting
    the specified CPU/RAM allocations. This task
    can utilize fewer cpu cores (62) than `custom_task`s (95)
    but can use more RAM (up to 490 GiB) than `custom_task`s (up to 179 GiB).
    This is ideal for processes which utilize a lot of memory per thread.
    Args:
        cpu: An integer number of cores to request, up to 63 cores
        memory: An integer number of Gibibytes of RAM to request, up to 511 GiB
    """
    warn(
        "`custom_memory_optimized_task` is deprecated and will be removed in a future"
        " release: use `custom_task` instead",
        DeprecationWarning,
        stacklevel=2,
    )
    if cpu > 62:
        raise ValueError(
            f"custom memory optimized task requires too many CPU cores: {cpu} (max 62)"
        )
    elif memory > 490:
        raise ValueError(
            f"custom memory optimized task requires too much RAM: {memory} GiB (max 490"
            " GiB)"
        )

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": str(cpu), "memory": f"{memory}Gi"},
        limits={"cpu": str(cpu), "memory": f"{memory}Gi"},
    )
    primary_container.resources = resources
    task_config = Pod(
        annotations={
            "io.kubernetes.cri-o.userns-mode": (
                "private:uidmapping=0:1048576:65536;gidmapping=0:1048576:65536"
            )
        },
        pod_spec=V1PodSpec(
            runtime_class_name="sysbox-runc",
            containers=[primary_container],
            tolerations=[
                V1Toleration(effect="NoSchedule", key="ng", value="mem-512-spot")
            ],
        ),
        primary_container_name="primary",
    )
    return functools.partial(task(task_config=task_config))


def custom_task(cpu: int, memory: int, *, storage_gib: int = 500):
    """Returns a custom task configuration requesting
    the specified CPU/RAM allocations

    Args:
        cpu: An integer number of cores to request, up to 95 cores
        memory: An integer number of Gibibytes of RAM to request, up to 490 GiB
        storage: An integer number of Gibibytes of storage to request, up to 4949 GiB
    """
    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={
            "cpu": str(cpu),
            "memory": f"{memory}Gi",
            "ephemeral-storage": f"{storage_gib}Gi",
        },
        limits={
            "cpu": str(cpu),
            "memory": f"{memory}Gi",
            "ephemeral-storage": f"{storage_gib}Gi",
        },
    )
    primary_container.resources = resources
    if cpu <= 31 and memory <= 127 and storage_gib <= 1949:
        task_config = Pod(
            annotations={
                "io.kubernetes.cri-o.userns-mode": (
                    "private:uidmapping=0:1048576:65536;gidmapping=0:1048576:65536"
                )
            },
            pod_spec=V1PodSpec(
                runtime_class_name="sysbox-runc",
                containers=[primary_container],
                tolerations=[
                    V1Toleration(effect="NoSchedule", key="ng", value="cpu-32-spot")
                ],
            ),
            primary_container_name="primary",
        )
    elif cpu <= 95 and memory <= 179 and storage_gib <= 4949:
        task_config = Pod(
            annotations={
                "io.kubernetes.cri-o.userns-mode": (
                    "private:uidmapping=0:1048576:65536;gidmapping=0:1048576:65536"
                )
            },
            pod_spec=V1PodSpec(
                runtime_class_name="sysbox-runc",
                containers=[primary_container],
                tolerations=[
                    V1Toleration(effect="NoSchedule", key="ng", value="cpu-96-spot")
                ],
            ),
            primary_container_name="primary",
        )
    elif cpu <= 62 and memory <= 490 and storage_gib <= 4949:
        task_config = Pod(
            annotations={
                "io.kubernetes.cri-o.userns-mode": (
                    "private:uidmapping=0:1048576:65536;gidmapping=0:1048576:65536"
                )
            },
            pod_spec=V1PodSpec(
                runtime_class_name="sysbox-runc",
                containers=[primary_container],
                tolerations=[
                    V1Toleration(effect="NoSchedule", key="ng", value="mem-512-spot")
                ],
            ),
            primary_container_name="primary",
        )
    else:
        if memory > 490:
            raise ValueError(
                f"custom task requires too much RAM: {memory} GiB (max 490 GiB)"
            )
        elif storage_gib > 4949:
            raise ValueError(
                f"custom task requires too much storage: {storage_gib} GiB (max 4949"
                " GiB)"
            )
        elif cpu > 95:
            raise ValueError(f"custom task requires too many CPU cores: {cpu} (max 95)")
        elif memory > 179 and cpu > 62:
            raise ValueError(
                f"could not resolve cpu for high memory machine: requested {cpu} cores"
                " (max 62)"
            )
        elif cpu > 62 and memory > 179:
            raise ValueError(
                f"could not resolve memory for high cpu machine: requested {memory} GiB"
                " (max 179 GiB)"
            )
        else:
            raise ValueError(
                f"custom task resource limit is too high: {cpu} (max 95) cpu cores,"
                f" {memory} GiB (max 179 GiB) memory, or {storage_gib} GiB storage (max"
                " 4949 GiB)"
            )

    return functools.partial(task(task_config=task_config))
