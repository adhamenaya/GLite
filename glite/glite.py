class GLite:
    def __init__(self):
        self.nodes = []
        self.edges = []

    class Node:
        def __init__(self, node_id, node_type=None, properties=None):
            self.id = node_id
            self.type = node_type
            self.properties = properties or {}

    class Edge:
        def __init__(self, edge_id, source_id, target_id, edge_type, properties=None):
            self.id = edge_id
            self.source = source_id
            self.target = target_id
            self.type = edge_type
            self.properties = properties or {}

    # Add node using plain parameters
    def add_node(self, node_id, node_type=None, properties=None):
        node = self.Node(node_id, node_type, properties)
        self.nodes.append(node)
        return node  # Optional: return the created node

    # Add edge using plain parameters
    def add_edge(self, edge_id, source_id, target_id, edge_type, properties=None):
        edge = self.Edge(edge_id, source_id, target_id, edge_type, properties)
        self.edges.append(edge)
        return edge  # Optional: return the created edge

    def find_nodes(self, node_type=None, properties=None):
        return [
            node for node in self.nodes
            if (not node_type or node.type == node_type)
               and all(node.properties.get(k) == v for k, v in (properties or {}).items())
        ]

    def find_edges(self, source_id=None, edge_type=None):
        return [
            edge for edge in self.edges
            if (not edge_type or edge.type == edge_type)
               and (source_id is None or edge.source == source_id)
        ]
