from contextlib import contextmanager
from multiprocessing import BoundedSemaphore
from multiprocessing.managers import BaseManager
from typing import Dict, List, Optional

import tqdm


def get_progress_bar():
    return tqdm.tqdm(
        total=0,
        leave=False,
        smoothing=0,
        unit="B",
        unit_scale=True,
    )


class ProgressBars:
    def __init__(
        self,
        num_task_bars: int,
        *,
        show_total_progress: bool = True,
        verbose: bool = False,
    ):
        if show_total_progress:
            self.total_bar = get_progress_bar()
            self.total_bar.desc = "Copying Files"
            self.total_bar.colour = "green"
            self.total_bar.unit = ""
        else:
            self.total_bar = None

        self.total_sema = BoundedSemaphore(1)

        self.verbose = verbose

        self.task_bars: List[tqdm.tqdm] = [
            get_progress_bar() for _ in range(num_task_bars)
        ]
        self.free_indices = {i for i in range(num_task_bars)}
        self.task_bar_sema = BoundedSemaphore(num_task_bars)

        self.usage: Dict[str, int] = {}

    def num_bars(self) -> int:
        return len(self.task_bars)

    def write(self, msg: str):
        if not self.verbose:
            return

        tqdm.tqdm.write(msg)

    def get_free_task_bar_index(self) -> Optional[int]:
        if len(self.task_bars) == 0:
            return None

        self.task_bar_sema.acquire(block=True)
        return self.free_indices.pop()

    def return_task_bar(self, index: Optional[int]):
        if index is None:
            return

        self.reset(index)
        self.free_indices.add(index)
        self.task_bar_sema.release()

    def set_usage(self, key: str, amount: int):
        self.usage[key] = amount

    def dec_usage(self, key: str):
        if key not in self.usage:
            return

        amount = self.usage[key]
        amount -= 1
        self.usage[key] = amount
        return amount

    def set_total(self, total: int, desc: Optional[str] = None):
        if self.total_bar is None:
            return

        self.total_bar.total = total
        if desc is not None:
            self.total_bar.desc = desc

    def update_total_progress(self, amount: int):
        if self.total_bar is None:
            return

        with self.total_sema:
            self.total_bar.update(amount)
            self.total_bar.refresh()

    def set(self, index: Optional[int], total: int, desc: str):
        if index is None:
            return

        self.task_bars[index].total = total
        self.task_bars[index].desc = desc
        self.task_bars[index].refresh()

    def update(self, index: Optional[int], amount: int):
        if index is None:
            return

        self.task_bars[index].update(amount)
        self.task_bars[index].refresh()

    def reset(self, index: Optional[int]):
        if index is None:
            return

        self.task_bars[index].reset()
        self.task_bars[index].total = 0
        self.task_bars[index].desc = ""
        self.task_bars[index].refresh()

    def close(self):
        if self.total_bar is not None:
            self.total_bar.close()

        for bar in self.task_bars:
            bar.close()


class ProgressBarManager(BaseManager):
    ...


ProgressBarManager.register("ProgressBars", ProgressBars)


@contextmanager
def get_free_index(progress_bars: ProgressBars):
    try:
        pbar_index = progress_bars.get_free_task_bar_index()
        yield pbar_index
    finally:
        progress_bars.return_task_bar(pbar_index)
