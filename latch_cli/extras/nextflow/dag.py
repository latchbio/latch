try:
    from functools import cache
except ImportError:
    from functools import lru_cache as cache

import json
import sys
import textwrap
from dataclasses import asdict, dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, TypedDict

from typing_extensions import NotRequired, Self

from latch_cli.utils import identifier_from_str


class VertexType(str, Enum):
    Process = "Process"
    Operator = "Operator"
    SubWorkflow = "SubWorkflow"
    Generator = "Generator"
    Conditional = "Conditional"
    Merge = "Merge"
    Input = "Input"


@dataclass(frozen=True)
class Vertex:
    id: str
    label: str
    type: VertexType
    statement: str
    ret: List[str] = field(hash=False)
    outputNames: List[str] = field(hash=False)
    module: str
    unaliased: str
    subWorkflowName: str
    subWorkflowPath: str
    cpu: Optional[int] = None
    memoryBytes: Optional[int] = None


@dataclass(frozen=True)
class Edge:
    label: str
    src: str
    dest: str
    branch: Optional[bool] = None


class _VertexContentJson(TypedDict):
    id: str
    label: str
    type: VertexType
    statement: str
    ret: List[str]
    outputNames: List[str]
    module: str
    unaliased: str
    subWorkflowName: str
    subWorkflowPath: str
    cpu: Optional[int]
    memoryBytes: Optional[int]


class _VertexJson(TypedDict):
    content: _VertexContentJson


class _EdgeContentJson(TypedDict):
    label: str
    src: str
    dest: str
    branch: NotRequired[Optional[bool]]


class _EdgeJson(TypedDict):
    content: _EdgeContentJson


class _DAGJson(TypedDict):
    vertices: List[_VertexJson]
    edges: List[_EdgeJson]


@dataclass(frozen=True)
class DAG:
    vertices: List[Vertex] = field(hash=False)
    edges: List[Edge] = field(hash=False)

    @classmethod
    def from_path(cls, p: Path) -> Self:
        if not p.exists():
            raise  # todo(ayush): better errors

        payload: _DAGJson = json.loads(p.read_text())

        vertices: List[Vertex] = []
        for v in payload["vertices"]:
            c = v["content"]
            c["label"] = textwrap.shorten(
                identifier_from_str(c["label"]),
                64,
                placeholder="",
            )
            vertices.append(Vertex(**c))

        edges: List[Edge] = []
        edge_set: Set[Tuple[str, str]] = set()
        for e in payload["edges"]:
            c = e["content"]
            t = (c["src"], c["dest"])

            edge = Edge(**c)
            edges.append(edge)
            edge_set.add(t)

        return cls(vertices, edges)

    @cache
    def _vertices_by_id(self) -> Dict[str, Vertex]:
        res: Dict[str, Vertex] = {}
        for v in self.vertices:
            res[v.id] = v

        return res

    @cache
    def src(self, e: Edge) -> Vertex:
        return self._vertices_by_id()[e.src]

    @cache
    def dest(self, e: Edge) -> Vertex:
        return self._vertices_by_id()[e.dest]

    @cache
    def ancestors(self) -> Dict[Vertex, List[Tuple[Vertex, Edge]]]:
        res: Dict[Vertex, List[Tuple[Vertex, Edge]]] = {}
        for v in self.vertices:
            res[v] = []

        by_id = self._vertices_by_id()
        for edge in self.edges:
            res[by_id[edge.dest]].append((by_id[edge.src], edge))

        return res

    @cache
    def inbound_edges(self) -> Dict[Vertex, List[Edge]]:
        res: Dict[Vertex, List[Edge]] = {}
        for v in self.vertices:
            res[v] = []

        by_id = self._vertices_by_id()
        for edge in self.edges:
            res[by_id[edge.dest]].append(edge)

        return res

    @cache
    def descendants(self) -> Dict[Vertex, List[Tuple[Vertex, Edge]]]:
        res: Dict[Vertex, List[Tuple[Vertex, Edge]]] = {}
        for v in self.vertices:
            res[v] = []

        by_id = self._vertices_by_id()
        for edge in self.edges:
            res[by_id[edge.src]].append((by_id[edge.dest], edge))

        return res

    @cache
    def outbound_edges(self) -> Dict[Vertex, List[Edge]]:
        res: Dict[Vertex, List[Edge]] = {}
        for v in self.vertices:
            res[v] = []

        by_id = self._vertices_by_id()
        for edge in self.edges:
            res[by_id[edge.src]].append(edge)

        return res

    @property
    @cache
    def source_vertices(self) -> List[Vertex]:
        res: List[Vertex] = []

        for v, upstream in self.ancestors().items():
            if len(upstream) != 0:
                continue

            res.append(v)

        return res

    @property
    @cache
    def sink_vertices(self) -> List[Vertex]:
        res: List[Vertex] = []

        for v, downstream in self.descendants().items():
            if len(downstream) != 0:
                continue

            res.append(v)

        return res

    @classmethod
    def _resolve_subworkflows_helper(
        cls,
        wf_name: str,
        dags: Dict[str, Self],
        sub_wf_dependencies: Dict[str, List[str]],
    ):
        for dep in sub_wf_dependencies[wf_name]:
            cls._resolve_subworkflows_helper(dep, dags, sub_wf_dependencies)

        dag = dags[wf_name]

        new_vertices: List[Vertex] = []
        new_edges: List[Edge] = []
        for v in dag.vertices:
            if v.type != VertexType.SubWorkflow:
                new_vertices.append(v)
                continue

            sub_dag = dags[v.label]
            for sub_v in sub_dag.vertices:
                args = asdict(sub_v)
                args["id"] = "_".join([v.id, sub_v.id])

                new_vertices.append(Vertex(**args))

            for sub_e in sub_dag.edges:
                new_edges.append(
                    Edge(
                        label=sub_e.label,
                        src="_".join([v.id, sub_e.src]),
                        dest="_".join([v.id, sub_e.dest]),
                        branch=sub_e.branch,
                    )
                )

        ids = set(v.id for v in new_vertices)
        for e in dag.edges:
            if e.src in ids:
                srcs = [e.src]
            else:
                sub_dag = dags[dag.src(e).label]

                srcs = ["_".join([e.src, v.id]) for v in sub_dag.sink_vertices]

            if e.dest in ids:
                dests = [e.dest]
            else:
                sub_dag = dags[dag.dest(e).label]

                dests = ["_".join([e.dest, v.id]) for v in sub_dag.source_vertices]

            for src in srcs:
                for dest in dests:
                    new_edges.append(
                        Edge(
                            label=e.label,
                            src=src,
                            dest=dest,
                            branch=e.branch,
                        )
                    )

        dags[wf_name] = cls(new_vertices, new_edges)

    @classmethod
    def resolve_subworkflows(cls, dags: Dict[str, Self]) -> Dict[str, Self]:
        dependencies: Dict[str, List[str]] = {}
        sources = set(dags.keys())

        for wf_name, dag in dags.items():
            deps: List[str] = []
            for v in dag.vertices:
                if v.type != VertexType.SubWorkflow:
                    continue

                deps.append(v.label)
                sources.discard(v.label)

            dependencies[wf_name] = deps

        # todo(ayush): idk the time/space complexity of this but its certainly not great
        resolved_dags = dags.copy()
        res: Dict[str, Self] = {}
        for source in sources:
            cls._resolve_subworkflows_helper(source, resolved_dags, dependencies)
            res[source] = resolved_dags[source]

        return res

    def _toposort_helper(self, cur: Vertex, res: List[Vertex], visited: Set[Vertex]):
        for x, _ in self.ancestors()[cur]:
            if x in visited:
                continue

            self._toposort_helper(x, res, visited)

        visited.add(cur)
        res.append(cur)

    @cache
    def toposorted(self) -> List[Vertex]:
        res = []
        visited = set()

        for sink in self.sink_vertices:
            self._toposort_helper(sink, res, visited)

        return res
