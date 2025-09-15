import ast
from inspect import getsource


class MethodAnalyzer(ast.NodeVisitor):
    """Collect self.method calls and self.attr reads (excluding set-then-read)."""

    def __init__(self, class_methods):
        self.class_methods = class_methods
        self.called = set()
        self.reads = set()
        self.writes = set()

    def visit_Call(self, node):
        f = node.func
        if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name):
            if f.value.id == "self" and f.attr in self.class_methods:
                self.called.add(f.attr)
        self.generic_visit(node)

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.value.id == "self":
            if isinstance(node.ctx, ast.Store):
                self.writes.add(node.attr)
            elif isinstance(node.ctx, ast.Load) and node.attr not in self.writes:
                self.reads.add(node.attr)
        self.generic_visit(node)

    def visit_FunctionDef(self, node):
        for stmt in node.body:
            self.visit(stmt)


def build_class_graph(cls):
    """Return {method_name: {"internal": [...]}} for instance methods."""
    tree = ast.parse(getsource(cls))

    # Find the class
    target_cls = None
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == cls.__name__:
            target_cls = node
            break
    if target_cls is None:
        raise ValueError(f"Class '{cls.__name__}' not found")

    # Only include instance methods defined directly on the class (sync functions)
    class_methods = {n.name for n in target_cls.body if isinstance(n, ast.FunctionDef)}

    graph = {}
    for n in target_cls.body:
        if isinstance(n, ast.FunctionDef):
            a = MethodAnalyzer(class_methods)
            a.visit(n)
            graph[n.name] = {"internal": sorted(a.called | a.reads)}
    return graph
