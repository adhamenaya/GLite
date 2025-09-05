from .glite_ast import *

class GLiteEvaluator:
    def __init__(self, graph, pattern):
        self.graph = graph
        self.DEBUG = True  # Enable debug output
        if pattern is None:
            raise ValueError("Path pattern cannot be None.")
        else:
            self.path_pattern = pattern

    def debug(self, msg):
        if self.DEBUG:
            print("[DEBUG]", msg)

    def evaluate(self, params=None):
        matches = []
        self.debug(f"Evaluating pattern: {self.path_pattern}")

        start_pattern = self.path_pattern.expr.nodes[0]
        start_candidates = self.match_nodes(start_pattern)

        self.debug(f"Start candidates (matching first node pattern '{start_pattern.filler.type}'): {[n.id for n in start_candidates]}")

        for start_node in start_candidates:
            bindings = {}
            if start_pattern.filler.variable:
                bindings[start_pattern.filler.variable] = start_node

            self.match_path_recursive(
                self.path_pattern.expr,
                node_index=1,
                current_node=start_node,
                current_bindings=bindings,
                path_nodes=[start_node],
                path_edges=[],
                all_matches=matches
            )

        if self.path_pattern.where:
            matches = [m for m in matches if self.evaluate_where(self.path_pattern.where, m, params)]
        if self.path_pattern.return_clause:
            matches = self.project_results(matches, self.path_pattern.return_clause)

        self.debug(f"Total matches found: {len(matches)}")
        return matches

    def project_results(self, matches, return_clause):
        projected_results = []

        has_functions = any(isinstance(item, FunctionExpr) for item in return_clause.items)
        function_values = {}

        # Compute function expressions first
        for item in return_clause.items:
            if not isinstance(item, FunctionExpr):
                continue

            func_name = item.name.upper()
            arg = item.args[0] if item.args else None
            key = item.alias or f"{func_name}({arg})"

            if func_name == "COUNT":
                if arg == "*":
                    val = len(matches)
                else:
                    val = sum(1 for m in matches if arg in m)
                function_values[key] = val
            else:
                raise NotImplementedError(f"Function '{func_name}' is not supported.")

        # Process individual matches if no function or multiple items
        if not has_functions or len(return_clause.items) > 1:
            for _match in matches:
                result = {}
                for item in return_clause.items:
                    if isinstance(item, FunctionExpr):
                        key = item.alias or f"{item.name.upper()}({item.args[0]})"
                        result[key] = function_values.get(key)
                        continue

                    if item.variable not in _match:
                        continue

                    node_or_edge = _match[item.variable]

                    # Determine key
                    if item.alias:
                        key = item.alias
                    elif item.attribute:
                        key = f"{item.variable}.{item.attribute}"
                    else:
                        key = item.variable

                    # Extract value
                    if item.attribute:
                        value = getattr(node_or_edge, "properties", {}).get(item.attribute)
                    else:
                        value = node_or_edge

                    result[key] = value

                projected_results.append(result)
        else:
            projected_results.append(function_values)

        return projected_results

    def match_path_recursive(self, expr, node_index, current_node, current_bindings, path_nodes, path_edges, all_matches):
        if node_index >= len(expr.nodes):
            all_matches.append(dict(current_bindings))
            self.debug(f"Match found: {current_bindings}")
            return

        edge_pattern = expr.edges[node_index - 1]
        next_pattern = expr.nodes[node_index]

        self.debug(f"Current node: {current_node.id}")
        candidates = self.match_edges(current_node, edge_pattern)

        self.debug(f"Edge candidates from node {current_node.id} matching pattern '{edge_pattern.filler.type}': {[(e.id, n.id) for e, n in candidates]}")

        for edge, next_node in candidates:
            if next_node.id in [n.id for n in path_nodes]:
                continue  # Cycle protection

            if next_pattern.filler.variable and next_pattern.filler.variable in current_bindings:
                continue

            new_bindings = dict(current_bindings)
            if edge_pattern.filler.variable:
                new_bindings[edge_pattern.filler.variable] = edge
            if next_pattern.filler.variable:
                new_bindings[next_pattern.filler.variable] = next_node

            self.match_path_recursive(
                expr,
                node_index + 1,
                next_node,
                new_bindings,
                path_nodes + [next_node],
                path_edges + [edge],
                all_matches=all_matches
            )

    def match_nodes(self, node_pattern):
        result = []
        for n in self.graph.nodes:
            # Case-insensitive type matching
            if node_pattern.filler.type and (node_pattern.filler.type.lower() != (n.type or "").lower()):
                continue
            if not self.match_props(n.properties, node_pattern.filler.properties):
                continue
            result.append(n)

        self.debug(f"Nodes matching pattern '{node_pattern.filler.type}': {[n.id for n in result]}")
        return result

    def match_edges(self, from_node, edge_pattern):
        matches = []
        direction = edge_pattern.direction

        for e in self.graph.edges:
            valid = (
                    (direction == "right" and e.source == from_node.id) or
                    (direction == "left" and e.target == from_node.id) or
                    (direction == "undirected" and (e.source == from_node.id or e.target == from_node.id))
            )

            if not valid:
                continue

            # Case-insensitive type matching
            if edge_pattern.filler.type and (edge_pattern.filler.type.lower() != (e.type or "").lower()):
                continue

            if not self.match_props(e.properties, edge_pattern.filler.properties):
                continue

            # Determine target node
            if direction == "right":
                target_node = next((n for n in self.graph.nodes if n.id == e.target), None)
            elif direction == "left":
                target_node = next((n for n in self.graph.nodes if n.id == e.source), None)
            else:  # undirected
                target_node = next((n for n in self.graph.nodes if n.id == (e.target if e.source == from_node.id else e.source)), None)

            if target_node:
                matches.append((e, target_node))

        self.debug(f"Matched edges from node {from_node.id}: {[(e.id, n.id) for e, n in matches]}")
        return matches

    def match_props(self, data_props, pattern_props):
        for k, v in pattern_props.items():
            if k not in data_props or str(data_props[k]) != str(v):
                return False
        return True

    def evaluate_where(self, where_clause, bindings, params):
        node = bindings.get(where_clause.variable)
        if not node:
            self.debug("No node in WHERE clause")
            return False

        val = node.properties.get(where_clause.attribute)
        if val is None:
            self.debug("No value in node for WHERE clause attribute")
            return False

        target = where_clause.value

        if isinstance(target, str) and target.startswith("${") and target.endswith("}"):
            param_name = target[2:-1]
            if param_name and param_name in params:
                target = params[param_name]
            else:
                self.debug(f"Parameter '{param_name}' not found")
                return False

        # Normalize types
        try:
            if isinstance(target, str):
                try:
                    target = int(target)
                except ValueError:
                    try:
                        target = float(target)
                    except ValueError:
                        pass
            if isinstance(val, str) and isinstance(target, (int, float)):
                val = type(target)(val)
        except Exception:
            pass

        op = where_clause.operator.upper()

        try:
            if op == "=":
                return val == target
            elif op == "!=":
                return val != target
            elif op == "<":
                return val < target
            elif op == ">":
                return val > target
            elif op == "<=":
                return val <= target
            elif op == ">=":
                return val >= target
            elif op == "NEAR":
                return val == target
            elif op == "IN":
                return val in target
            elif op == "NOT_IN":
                return val not in target
        except Exception as e:
            self.debug(f"Error in WHERE comparison: {e}")
            return False

        return False
