from dataclasses import dataclass
from typing import Any, Optional

@dataclass(frozen=True)
class WidgetValue:
    value: Any

@dataclass(frozen=True)
class PlotsArtifactTemplate:
    id: str
    widgetValues: Optional[dict[str, WidgetValue]] = None

@dataclass(frozen=True)
class PlotsArtifactBindings:
    plotTemplates: list[PlotsArtifactTemplate]

@dataclass(frozen= True)
class PlotsArtifact:
    bindings: PlotsArtifactBindings
