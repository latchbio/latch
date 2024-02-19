from multiprocessing.managers import SyncManager
from typing import Type

from .progress import _ProgressBars
from .throttle import _Throttle


class _TransferStateManager(SyncManager):
    ProgressBars: Type[_ProgressBars]
    Throttle: Type[_Throttle]


_TransferStateManager.register("ProgressBars", _ProgressBars)
_TransferStateManager.register("Throttle", _Throttle)
