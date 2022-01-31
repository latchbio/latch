from flytekit import task
from flytekitplugins.pod import Pod
from kubernetes.client.models import (V1Container, V1PodSpec, V1ResourceRequirements, V1Toleration)

def _get_large_gpu_pod() -> Pod:
	"""Returns a pod which will be scheduled on a gpu node with at least 31 cpus and 120 gigs of memory
	"""
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
		primary_container_name="primary"
	)

def _get_medium_gpu_pod() -> Pod:
	"""Returns a pod which will be scheduled on a gpu node with at least 6 cpus and 31 gigs of memory
	"""
	primary_container = V1Container(name="primary")
	resources = V1ResourceRequirements(
		requests={"cpu": "6", "memory": "31", "nvidia.com/gpu": "1"},
		limits={"cpu": "8", "memory": "32", "nvidia.com/gpu": "1"},
	)
	primary_container.resources = resources

	return Pod(
		pod_spec=V1PodSpec(
			containers=[primary_container],
			tolerations=[V1Toleration(effect="NoSchedule", key="ng", value="gpu-big")],
		),
		primary_container_name="primary"
	)

def _get_large_pod() -> Pod:
	"""Returns a pod which will be scheduled on a node with at least 31 cpus and 120 gigs of memory
	"""
	primary_container = V1Container(name="primary")
	resources = V1ResourceRequirements(
		requests={"cpu": "31", "memory": "120Gi"},
		limits={"cpu": "32", "memory": "128Gi"},
	)
	primary_container.resources = resources

	return Pod(
		pod_spec=V1PodSpec(
			containers=[primary_container],
			tolerations=[V1Toleration(effect="NoSchedule", key="ng", value="big")],
		),
		primary_container_name="primary"
	)

def _get_medium_pod() -> Pod:
	"""Returns a pod which will be scheduled on a node with at least 8 cpus and 32 gigs of memory
	"""

	primary_container = V1Container(name="primary")
	resources = V1ResourceRequirements(
		requests={"cpu": "8", "memory": "32Gi"},
		limits={"cpu": "12", "memory": "64Gi"},
	)
	primary_container.resources = resources

	return Pod(
		pod_spec=V1PodSpec(
			containers=[primary_container],
			tolerations=[V1Toleration(effect="NoSchedule", key="ng", value="on-demand-medium")],
		),
		primary_container_name="primary"
	)

def _get_small_pod() -> Pod:
	"""Returns a pod which will be scheduled on a node with at least 31 cpus and 120 gigs of memory
	"""
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
		primary_container_name="primary"
	)

large_gpu_task = task(task_config=_get_large_gpu_pod())
medium_gpu_task = task(task_config=_get_medium_gpu_pod())
large_task = task(task_config=_get_large_pod())
medium_task = task(task_config=_get_medium_pod())
small_task = task(task_config=_get_small_pod())
