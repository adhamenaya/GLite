# 1- Define AST nodes
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Literal, Union

EdgeDirection = Literal["left", "right", "undirected"]

@dataclass
class ElementFiller:
    variable: Optional[str] = None
    _type: Optional[str] = None
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


# 2- Parser
class GLiteParser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    # --- Helper methods ---
    def current_token(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else ("EOF", "")

    def peek(self, kind, offset=0):
        idx = self.pos + offset
        return idx < len(self.tokens) and self.tokens[idx][0] == kind

    def peek_next(self, kind):
        return self.peek(kind, 1)

    def match(self, kind):
        if self.peek(kind):
            self.pos += 1
            return self.tokens[self.pos - 1][1]
        return None

    def expect(self, kind):
        if self.peek(kind):
            value = self.match(kind)
            return value
        actual = self.current_token()[0]
        raise SyntaxError(f"Expected {kind} at position {self.pos}, got {actual}")

    # --- Parsing methods ---
    def parse_element_filler(self):
        var = self.match("IDENT")
        _type = None
        props = {}

        if self.match("COLON"):
            _type = self.expect("IDENT")

        if self.match("LBRACE"):
            while True:
                key = self.expect("IDENT")
                self.expect("COLON")
                if self.peek("STRING"):
                    val = self.match("STRING").strip('"').strip("'")
                elif self.peek("NUMBER"):
                    val = self.match("NUMBER")
                    val = int(val) if val.isdigit() else float(val)
                elif self.peek("IDENT"):
                    val = self.match("IDENT")
                else:
                    raise SyntaxError(f"Expected STRING, NUMBER, or IDENT at position {self.pos}")
                props[key] = val
                if not self.match("COMMA"):
                    break
            self.expect("RBRACE")

        return ElementFiller(var, _type, props)

    def parse_node_pattern(self):
        self.expect("LPAREN")
        filler = self.parse_element_filler()
        self.expect("RPAREN")
        return NodePattern(filler)

    def parse_edge_pattern(self):
        direction = "undirected"
        if self.match("ARROW_LEFT"):
            direction = "left"
            self.expect("LBRACK")
            filler = self.parse_element_filler()
            self.expect("RBRACK")
            self.expect("DASH")
        else:
            self.expect("DASH")
            self.expect("LBRACK")
            filler = self.parse_element_filler()
            self.expect("RBRACK")
            if self.match("ARROW_RIGHT"):
                direction = "right"
        return EdgePattern(filler, direction)

    def parse_comparison_expr(self):
        var = self.expect("IDENT")
        self.expect("DOT")
        attr = self.expect("IDENT")

        op = None
        for kind in ("EQ", "NEQ", "LTE", "GTE", "LT", "GT", "NEAR", "IN", "NOT_IN"):
            op_val = self.match(kind)
            if op_val:
                op = op_val
                break
        if not op:
            raise SyntaxError(f"Expected comparison operator at position {self.pos}")

        if self.peek("PARAM"):
            val = self.match("PARAM")
        elif self.peek("IDENT"):
            val = self.match("IDENT")
        elif self.peek("NUMBER"):
            val = self.match("NUMBER")
            val = int(val) if val.isdigit() else float(val)
        elif self.peek("STRING"):
            val = self.match("STRING").strip('"').strip("'")
        else:
            raise SyntaxError(f"Expected IDENT, NUMBER or STRING at position {self.pos}")

        return ComparisonExpr(var, attr, op, val)

    def parse_function_expr(self, func_name):
        self.match("IDENT")
        self.expect("LPAREN")
        args = []
        while not self.peek("RPAREN"):
            if self.match("STAR"):
                args.append("*")
            else:
                args.append(self.expect("IDENT"))
            self.match("COMMA")  # optional comma
        self.expect("RPAREN")
        alias = self.match("AS") and self.expect("IDENT")
        return FunctionExpr(func_name.upper(), args, alias)

    def parse_return_item(self):
        # Function-based return item
        if self.peek("IDENT") and self.peek_next("LPAREN"):
            func_name = self.current_token()[1]
            return self.parse_function_expr(func_name)

        variable = self.expect("IDENT")
        attribute = None
        alias = None

        if self.match("DOT"):
            attribute = self.expect("IDENT")
        if self.match("AS"):
            alias = self.expect("IDENT")

        return ReturnItem(variable, attribute, alias)

    def parse_return_clause(self):
        self.expect("RETURN")
        distinct = bool(self.match("DISTINCT"))
        items = [self.parse_return_item()]
        while self.match("COMMA"):
            items.append(self.parse_return_item())
        return ReturnClause(items, distinct)

    def parse_path_pattern_expr(self):
        nodes = [self.parse_node_pattern()]
        edges = []

        while self.pos < len(self.tokens):
            if self.peek("WHERE") or self.peek("RETURN") or self.peek("WITH"):
                break
            if not (self.peek("DASH") or self.peek("ARROW_LEFT") or self.peek("ARROW_RIGHT")):
                break
            edge = self.parse_edge_pattern()
            node = self.parse_node_pattern()
            edges.append(edge)
            nodes.append(node)

        return PathPatternExpr(nodes, edges)

    def parse_path_pattern(self):
        self.expect("MATCH")
        expr = self.parse_path_pattern_expr()
        where = None
        if self.match("WHERE"):
            where = self.parse_comparison_expr()
        return_clause = None
        if self.peek("RETURN"):
            return_clause = self.parse_return_clause()
        return PathPattern(expr, where, return_clause)
