from dataclasses import dataclass, field, is_dataclass
from typing import Generic, List, TypeVar

T = TypeVar("T")


@dataclass
class Samplesheet(Generic[T]):
    samples: List[T] = field(default_factory=list)


@dataclass
class Sample:
    t: str


def f(s: Samplesheet) -> Samplesheet:
    s.samples.append(Sample(t="t"))
    print(s)


if __name__ == "__main__":
    s = Samplesheet()
    f(s)
