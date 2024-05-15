from enum import Enum


class Units(int, Enum):
    KiB = 2**10
    kB = 10**3

    MiB = 2**20
    MB = 10**6

    GiB = 2**30
    GB = 10**9

    TiB = 2**40
    TB = 10**12
