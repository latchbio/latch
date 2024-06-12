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

import datetime
import functools
from dataclasses import dataclass
from typing import Callable, Union
from warnings import warn

from flytekit import task
from flytekitplugins.pod import Pod
from kubernetes.client.models import (
    V1Container,
    V1PersistentVolumeClaimVolumeSource,
    V1PodSpec,
    V1ResourceRequirements,
    V1Toleration,
    V1Volume,
    V1VolumeMount,
)

from latch_cli.constants import Units

from .dynamic import DynamicTaskConfig


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
            "cpu": "7",
            "memory": "30Gi",
            "nvidia.com/gpu": "1",
            "ephemeral-storage": "1500Gi",
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
        limits={"cpu": "90", "memory": "170Gi", "ephemeral-storage": "4500Gi"},
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
    """[ "m5.8xlarge", "m5ad.8xlarge", "m5d.8xlarge", "m5n.8xlarge", "m5dn.8xlarge", "m5a.8xlarge" ]"""

    primary_container = V1Container(name="primary")
    resources = V1ResourceRequirements(
        requests={"cpu": "30", "memory": "100Gi", "ephemeral-storage": "1500Gi"},
        limits={"cpu": "30", "memory": "100Gi", "ephemeral-storage": "1500Gi"},
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
        limits={"cpu": "2", "memory": "4Gi", "ephemeral-storage": "100Gi"},
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
    but can use more RAM (up to 485 GiB) than `custom_task`s (up to 179 GiB).
    This is ideal for processes which utilize a lot of memory per thread.
    Args:
        cpu: An integer number of cores to request, up to 63 cores
        memory: An integer number of Gibibytes of RAM to request, up to 511 GiB
    """
    warn(
        "`custom_memory_optimized_task` is deprecated and will be removed in a"
        " future release: use `custom_task` instead",
        DeprecationWarning,
        stacklevel=2,
    )
    if cpu > 62:
        raise ValueError(
            f"custom memory optimized task requires too many CPU cores: {cpu} (max 62)"
        )
    elif memory > 485:
        raise ValueError(
            f"custom memory optimized task requires too much RAM: {memory} GiB (max 485"
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
    return functools.partial(task, task_config=task_config)


@dataclass
class _NGConfig:
    max_cpu_schedulable: int
    max_memory_schedulable_gib: int
    max_storage_schedulable_gib: int
    toleration_value: str


taint_data = [
    _NGConfig(30, 120, 2000, "cpu-32-spot"),
    _NGConfig(94, 176, 4949, "cpu-96-spot"),
    _NGConfig(62, 485, 4949, "mem-512-spot"),
    _NGConfig(126, 975, 4949, "mem-1tb"),
]

max_cpu = taint_data[-1].max_cpu_schedulable
max_memory_gib = taint_data[-1].max_memory_schedulable_gib
max_memory_gb_ish = int(max_memory_gib * Units.GiB / Units.GB)

max_storage_gib = taint_data[-1].max_storage_schedulable_gib
max_storage_gb_ish = int(max_storage_gib * Units.GiB / Units.GB)


def _custom_task_config(
    cpu: int,
    memory: int,
    storage_gib: int,
) -> Pod:
    target_ng = None
    for ng in taint_data:
        if (
            cpu <= ng.max_cpu_schedulable
            and memory <= ng.max_memory_schedulable_gib
            and storage_gib <= ng.max_storage_schedulable_gib
        ):
            target_ng = ng
            break

    if target_ng is None:
        raise ValueError(
            f"custom task request of {cpu} cores, {memory} GiB memory, and"
            f" {storage_gib} GiB storage exceeds the maximum allowed values of"
            f" {max_cpu} cores, {max_memory_gib} GiB memory ({max_memory_gb_ish} GB),"
            f" and {max_storage_gib} GiB storage ({max_storage_gb_ish} GB)"
        )

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
                V1Toleration(
                    effect="NoSchedule", key="ng", value=target_ng.toleration_value
                )
            ],
        ),
        primary_container_name="primary",
    )


def custom_task(
    cpu: Union[Callable, int],
    memory: Union[Callable, int],
    *,
    storage_gib: Union[Callable, int] = 500,
    timeout: Union[datetime.timedelta, int] = 0,
):
    """Returns a custom task configuration requesting
    the specified CPU/RAM allocations

    Args:
        cpu: An integer number of cores to request, up to 126 cores
        memory: An integer number of Gibibytes of RAM to request, up to 975 GiB
        storage: An integer number of Gibibytes of storage to request, up to 4949 GiB
    """
    if callable(cpu) or callable(memory) or callable(storage_gib):
        task_config = DynamicTaskConfig(
            cpu=cpu,
            memory=memory,
            storage=storage_gib,
            pod_config=_get_small_pod(),
        )
        return functools.partial(task, task_config=task_config, timeout=timeout)

    return functools.partial(
        task, task_config=_custom_task_config(cpu, memory, storage_gib), timeout=timeout
    )


def nextflow_runtime_task(cpu: int, memory: int, storage_gib: int = 50):
    task_config = _custom_task_config(cpu, memory, storage_gib)

    task_config.pod_spec.automount_service_account_token = True

    assert len(task_config.pod_spec.containers) == 1
    task_config.pod_spec.containers[0].volume_mounts = [
        V1VolumeMount(mount_path="/nf-workdir", name="nextflow-workdir")
    ]

    task_config.pod_spec.volumes = [
        V1Volume(
            name="nextflow-workdir",
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                # this value will be injected by flytepropeller
                claim_name="nextflow-pvc-placeholder"
            ),
        )
    ]

    return functools.partial(task, task_config=task_config)
