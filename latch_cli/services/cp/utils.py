import os
from typing import List


def get_max_workers() -> int:
    try:
        max_workers = len(os.sched_getaffinity(0)) * 4
    except AttributeError:
        cpu = os.cpu_count()
        if cpu is not None:
            max_workers = cpu * 4
        else:
            max_workers = 16

    return min(max_workers, 16)


def pluralize(singular: str, plural: str, selector: int) -> str:
    if selector == 1:
        return singular
    return plural


def human_readable_time(t_seconds: float) -> str:
    s = t_seconds % 60
    m = (t_seconds // 60) % 60
    h = t_seconds // 60 // 60

    x: List[str] = []
    if h > 0:
        x.append(f"{int(h):d}h")
    if m > 0:
        x.append(f"{int(m):d}m")
    if s > 0:
        x.append(f"{s:.2f}s")

    return " ".join(x)
