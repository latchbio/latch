from multiprocessing.managers import SyncManager
from typing import Type

from .progress import ProgressBars
from .throttle import Throttle


class TransferStateManager(SyncManager):
    ProgressBars: Type[ProgressBars]
    Throttle: Type[Throttle]


TransferStateManager.register("ProgressBars", ProgressBars)
TransferStateManager.register("Throttle", Throttle)
