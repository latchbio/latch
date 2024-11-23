from dataclasses import dataclass
from typing import Any, Optional, Union
import orjson


@dataclass(frozen=True)
class Widget:
    transform_id: str
    key: str
    value: Any


@dataclass(frozen=True)
class PlotsArtifactTemplate:
    template_id: str
    widgets: Optional[list[Widget]] = None


@dataclass(frozen=True)
class PlotsArtifactBindings:
    plot_templates: list[PlotsArtifactTemplate]


@dataclass(frozen=True)
class PlotsArtifact:
    bindings: PlotsArtifactBindings

    def asdict(self):
        d = {}
        bindings = d["bindings"] = {}
        plot_templates = bindings["plotTemplates"] = []

        for pt in self.bindings.plot_templates:
            template: dict[str, Union[str, dict[str, dict[str, Any]]]] = {
                "id": pt.template_id
            }

            if pt.widgets is not None:
                widget_values: dict[str, dict[str, Any]] = {}
                for w in pt.widgets:
                    widget_values[f"{w.transform_id}/{w.key}"] = {"value": w.value}

                template["widgetValues"] = widget_values

            plot_templates.append(template)

        return d

    def to_json(self) -> str:
        return orjson.dumps(self.asdict()).decode()
