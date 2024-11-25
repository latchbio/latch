from dataclasses import dataclass
from typing import Any, Optional, Union


@dataclass(frozen=True)
class Widget:
    transform_id: str
    key: str
    value: Any

    def asdict(self) -> dict:
        return {f"{self.transform_id}/{self.key}": {"value": self.value}}


@dataclass(frozen=True)
class PlotsArtifactTemplate:
    template_id: str
    widgets: Optional[list[Widget]] = None

    def asdict(self) -> dict:
        template: dict[str, Union[str, dict[str, dict[str, Any]]]] = {
            "id": self.template_id
        }

        if self.widgets is not None:
            widget_values: dict[str, dict[str, Any]] = {}
            for w in self.widgets:
                widget_values[f"{w.transform_id}/{w.key}"] = {"value": w.value}

            template["widgetValues"] = widget_values

        return template


@dataclass(frozen=True)
class PlotsArtifactBindings:
    plot_templates: list[PlotsArtifactTemplate]

    def asdict(self) -> dict:
        return {"plotTemplates": [pt.asdict() for pt in self.plot_templates]}


@dataclass(frozen=True)
class PlotsArtifact:
    bindings: PlotsArtifactBindings

    def asdict(self) -> dict:
        return {"bindings": self.bindings.asdict()}
