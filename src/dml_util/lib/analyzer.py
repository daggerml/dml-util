import ast
import os
import subprocess
from inspect import getsource, getsourcefile
from textwrap import dedent


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
    tree = ast.parse(dedent(getsource(cls)))

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


def get_dag_name(dag: type) -> str:
    """Get a dag name from the path of the file where the dag is defined.

    Example:
    If the repo is structured as follows:
    ```
    .
    ├── dags
    │   ├── foo/
    │   │   └── bar-dag.py  # => dag name: dags:foo:bar-dag
    │   ├── my_dag.py  # => dag name: dags:my_dag
    │   ├── my.dag.py  # => dag name: dags:my.dag
    │   └── another_dag.py  # => dag name: dags:another_dag
    └── main.py  # => dag name: main
    ```
    Note that this is all relative to the repo root.
    """
    file_path = getsourcefile(dag)
    assert file_path is not None
    file_path = os.path.splitext(file_path)[0]
    repo_root = os.getenv("DML_REPO_ROOT")
    if not repo_root:
        try:
            repo_root = (
                subprocess.check_output(["git", "rev-parse", "--show-toplevel"])
                .decode("utf-8")
                .strip()
            )
        except Exception:
            pass
    if not repo_root:
        repo_root = os.getcwd()
    rel_path = os.path.relpath(file_path, repo_root)
    parts = rel_path.split(os.sep)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ":".join(parts)


def topo_sort(dependencies: dict) -> list:
    """Topologically sort a dependency graph.

    Parameters
    ----------
    dependencies: dict
        A mapping of item names to a list of item names they depend on.

    Returns
    -------
    list
        A list of item names sorted such that each item appears after its dependencies.

    Raises
    -------
    ValueError
        If a cycle is detected in the dependency graph.
    """
    from collections import defaultdict, deque

    # Calculate in-degrees
    in_degree = defaultdict(int)
    for node, deps in dependencies.items():
        if node not in in_degree:
            in_degree[node] = 0
        for dep in deps:
            in_degree[dep] += 1
    # Initialize queue with nodes having zero in-degree
    queue = deque([node for node, degree in in_degree.items() if degree == 0])
    sorted_list = []
    while queue:
        node = queue.popleft()
        sorted_list.append(node)
        for neighbor in dependencies.get(node, []):
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)
    if len(sorted_list) != len(in_degree):
        raise ValueError("Cycle detected in dependency graph")
    return sorted_list
