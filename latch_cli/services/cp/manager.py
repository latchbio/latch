from multiprocessing.managers import SyncManager
from typing import Type

from .progress import ProgressBars
from .throttle import Throttle


class CPStateManager(SyncManager):
    ProgressBars: Type[ProgressBars]
    Throttle: Type[Throttle]


CPStateManager.register("ProgressBars", ProgressBars)
CPStateManager.register("Throttle", Throttle)
