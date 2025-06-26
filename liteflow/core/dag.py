class DAGNode:
    def __init__(self, name, inputs=None):
        self.name = name
        self.inputs = inputs or []

    def __repr__(self):
        return f"DAGNode({self.name})"
