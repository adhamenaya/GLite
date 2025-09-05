from dataclasses import dataclass, field
from typing import Optional, List, Dict, Literal, Union

EdgeDirection = Literal["left", "right", "undirected"]

@dataclass
class ElementFiller:
    variable: Optional[str] = None
    type: Optional[str] = None
    properties: Dict[str, str] = field(default_factory=dict)

@dataclass
class NodePattern:
    filler: ElementFiller

@dataclass
class EdgePattern:
    filler: ElementFiller
    direction: EdgeDirection = "undirected"

@dataclass
class PathPatternExpr:
    nodes: List[NodePattern]
    edges: List[EdgePattern]

@dataclass
class ComparisonExpr:
    variable: str
    attribute: str
    operator: str
    value: Union[str, int, float]  # Can extend with ValueExpr later

@dataclass
class FunctionExpr:
    name: str
    args: List[str]
    alias: Optional[str] = None

@dataclass
class ReturnItem:
    variable: str
    attribute: Optional[str] = None  # None for whole node/edge
    alias: Optional[str] = None

@dataclass
class ReturnClause:
    items: List[ReturnItem]
    distinct: bool = False

@dataclass
class PathPattern:
    expr: PathPatternExpr
    where: Optional[ComparisonExpr] = None
    return_clause: Optional[ReturnClause] = None