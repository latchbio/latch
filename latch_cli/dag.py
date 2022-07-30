from textwrap import indent
from typing import Dict, List, Optional, Tuple


class Node:
    def __init__(
        self,
        name: str,
        real: bool = True,  # whether this Node is real or not (for the purposes of positioning)
        pos: Optional[Tuple[int, int]] = None,
        height: Optional[int] = None,
        width: Optional[int] = None,
    ):
        """
        A Node object that contains position/dimension info about itself

        Args:
            pos: A tuple of integers of the form (x, y) where x and y are their respective coordinates
            height: The positive integer height of the box to be rendered, defaults to 4
            width: The positive integer width of the box to be rendered, defaults to height if not provided
        """
        if pos is None:
            pos = (0, 0)
        if width is None:
            width = len(name) + 5
        if height is None:
            height = 4

        self.pos = pos
        self.real = real
        self.name = name
        self.height = height
        self.width = width

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f"{self.name}: {self.width} x {self.height} @ {self.pos}"


class Graph:
    def __init__(self, node_names: List[str] = []):
        self.nodes: Dict[str, Node] = {}
        self.children: Dict[str, set[str]] = {}
        for node_name in node_names:
            self.nodes[node_name] = Node(name=node_name)
        self._curr_selected: Optional[str] = None

    def add_edge(self, node_0: str, node_1: str):
        """
        Adds an edge to the graph - if either of the nodes are not already
        in the graph, this method will also add them to the graph
        """
        if node_0 not in self.nodes:
            self.nodes[node_0] = Node(name=node_0)
        if node_1 not in self.nodes:
            self.nodes[node_1] = Node(name=node_1)

        if node_0 not in self.children:
            self.children[node_0] = set()
        self.children[node_0].add(node_1)

    def to_dot(self):
        if self._curr_selected is None:
            result_str = ""
        else:
            result_str = f"{self._curr_selected} [color = green]\n"
        for node_name in self.nodes:
            result_str += node_name
            if node_name in self.children:
                result_str += " -> {" + " ".join(self.children[node_name]) + "}"
            result_str += "\n"
        return "graph {\n" + indent(result_str, "    ") + "}"

    # @property
    # def curr_selected(self) -> Optional[str]:
    #     if self._curr_selected is not None:
    #         i, j = self._curr_selected
    #         return self.layers[i][j]
    #     return None

    # @property
    # def parents(self) -> Dict[str, set[str]]:
    #     parents: Dict[str, set[str]] = {}
    #     for node in self.nodes:
    #         for child in self.children.get(node, set()):
    #             if child not in parents:
    #                 parents[child] = set()
    #             parents[child].add(node)
    #     return parents

    # @property
    # def sorted_order(self) -> List[str]:
    #     """Returns a topological sort of the nodes"""
    #     sorted_order = []

    #     def dfs(node: str, visited: set[str]):
    #         if node in visited:
    #             raise ValueError("Cycle Detected")
    #         visited.add(node)
    #         for parent in self.parents.get(node, set()):
    #             dfs(parent, visited)
    #         visited.discard(node)
    #         nonlocal sorted_order
    #         sorted_order.append(node)

    #     for node in self.nodes:
    #         # O(n) check but doesnt really matter since n is small
    #         if node not in sorted_order:
    #             dfs(node, set())

    #     return sorted_order

    # @property
    # def layers(self) -> List[List[str]]:
    #     layer_nums: Dict[str, int] = {}
    #     total_layers = 0

    #     for node in self.sorted_order:
    #         layer_no = 0
    #         for parent in self.parents.get(node, set()):
    #             layer_no = max(layer_no, layer_nums[parent] + 1)
    #         layer_nums[node] = layer_no
    #         total_layers = max(layer_no + 1, total_layers)

    #     layers: List[List[str]] = [[] for _ in range(total_layers)]
    #     for node in layer_nums:
    #         layers[layer_nums[node]].append(node)

    #     return layers


if __name__ == "__main__":
    g = Graph()
    g.add_edge("chrome", "content")
    g.add_edge("chrome", "blink")
    g.add_edge("chrome", "base")
    g.add_edge("content", "blink")
    g.add_edge("content", "net")
    g.add_edge("content", "base")
    g.add_edge("blink", "v8")
    g.add_edge("blink", "CC")
    g.add_edge("blink", "WTF")
    g.add_edge("blink", "skia")
    g.add_edge("blink", "base")
    g.add_edge("blink", "net")
    g.add_edge("weblayer", "content")
    g.add_edge("weblayer", "chrome")
    g.add_edge("weblayer", "base")
    g.add_edge("net", "base")
    g.add_edge("WTF", "base")

    g._curr_selected = "WTF"

    print(g.to_dot())
