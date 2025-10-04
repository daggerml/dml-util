from __future__ import annotations

import ast
import dataclasses as dc
import logging
import os
import subprocess
from collections import defaultdict, deque
from dataclasses import InitVar, dataclass, fields
from functools import partial
from inspect import getmro, getsource, getsourcefile
from textwrap import dedent
from typing import Any, Callable, Optional, TypeVar

from daggerml import Dag as DmlDag
from daggerml import Dml, Executable, Node

from dml_util.funk import funkify

try:
    from typing import dataclass_transform  # 3.11+
except ImportError:  # 3.8–3.10
    from typing_extensions import dataclass_transform  # type: ignore

try:
    from typing import Concatenate, ParamSpec  # 3.10+
except ImportError:  # 3.8–3.9
    from typing_extensions import Concatenate, ParamSpec  # type: ignore


logger = logging.getLogger(__name__)
funk = partial(funkify, unpack_args=True)


class MethodAnalyzer(ast.NodeVisitor):
    def __init__(self, class_methods):
        self.class_methods = class_methods
        self.called = set()
        self.writes = set()
        self.reads = set()

    def visit_Call(self, node):
        f = node.func
        if (
            isinstance(f, ast.Attribute)
            and isinstance(f.value, ast.Name)
            and f.value.id == "self"
            and f.attr in self.class_methods
        ):
            self.called.add(f.attr)
        self.generic_visit(node)

    def visit_Attribute(self, node):
        if isinstance(node.value, ast.Name) and node.value.id == "self":
            (self.writes if isinstance(node.ctx, ast.Store) else self.reads).add(node.attr)
            if node.attr in self.writes and isinstance(node.ctx, ast.Load):
                self.reads.discard(node.attr)
        self.generic_visit(node)


def build_class_graph(cls):
    methods_ast = {}
    for k in getmro(cls):
        if k is object:
            continue
        try:
            tree = ast.parse(dedent(getsource(k)))
        except Exception:
            continue
        c = next((n for n in tree.body if isinstance(n, ast.ClassDef)), None)
        if not c:
            continue
        for n in c.body:
            if isinstance(n, ast.FunctionDef) and not n.name.startswith("_"):
                methods_ast.setdefault(n.name, n)
    class_methods = set(methods_ast)
    graph = {}
    for name, node in methods_ast.items():
        a = MethodAnalyzer(class_methods)
        a.visit(node)
        graph[name] = {"internal": sorted(a.called | a.reads)}
    return graph


def get_dag_name(dag: type) -> str:
    p = getsourcefile(dag)
    assert p is not None
    p = os.path.splitext(p)[0]
    root = os.getenv("DML_REPO_ROOT")
    if not root:
        try:
            root = subprocess.check_output(["git", "rev-parse", "--show-toplevel"]).decode().strip()
        except Exception:
            pass
    root = root or os.getcwd()
    parts = os.path.relpath(p, root).split(os.sep)
    if parts[-1] == "__init__":
        parts = parts[:-1]
    return ":".join(parts) + "::" + dag.__name__


def topo_sort(dependencies: dict) -> list:
    deg = defaultdict(int)
    for n, deps in dependencies.items():
        deg.setdefault(n, 0)
        for d in deps:
            deg[d] += 1
    q, out = deque([n for n, v in deg.items() if v == 0]), []
    while q:
        n = q.popleft()
        out.append(n)
        for m in dependencies.get(n, []):
            deg[m] -= 1
            if deg[m] == 0:
                q.append(m)
    if len(out) != len(deg):
        raise ValueError("Cycle detected in dependency graph")
    return out


def _copy_params(p) -> dict[str, Any]:
    keys = ("init", "repr", "eq", "order", "unsafe_hash", "frozen", "kw_only", "slots", "match_args")
    return {k: getattr(p, k) for k in keys if hasattr(p, k)}


@dataclass_transform()
class AutoDataclassBase:
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # If subclass is already explicitly a dataclass, leave it alone.
        if "__dataclass_params__" in cls.__dict__:
            return
        # Find nearest dataclass ancestor and copy its options (except init)
        params = None
        for b in cls.__mro__[1:]:
            p = b.__dict__.get("__dataclass_params__")
            if p is not None:
                params = p
                break
        opts = _copy_params(params) if params else {}
        opts["init"] = True  # <-- important: generate an __init__ for the subclass
        dc.dataclass(**opts)(cls)


RESERVED_WORDS = {"dag", "dml", "argv", "call", "put", "commit"}


def proc_deps(cls):
    deps = {}
    for k, v in build_class_graph(cls).items():
        if k in {"put", "commit", "call"}:
            continue
        if k in RESERVED_WORDS:
            raise ValueError(f"Field or method name {k!r} is reserved")
        deps[k] = []
        for x in sorted(set(v["internal"]) - RESERVED_WORDS):
            # for each dep, if it's not a field, method, nor prepop, error out
            if not hasattr(cls, x) and x not in getattr(getattr(cls, k), "prepop", []):
                raise ValueError(f"Method {k!r} depends on unknown field or method: {x!r}")
            # Only add if it's not a prepop
            if x not in getattr(getattr(cls, k), "prepop", []):
                deps[k].append(x)
    order = reversed(topo_sort({k: sorted(set(deps) & set(v)) for k, v in deps.items()}))
    return deps, order


_P = ParamSpec("P")
_T = TypeVar("_T")


class DelayedAction:
    def __init__(self, action: Callable[Concatenate[DmlDag, _P], _T], /, *args: _P.args, **kwargs: _P.kwargs):
        self.action = action
        self.args = args
        self.kwargs = kwargs

    def __call__(self, dag: DmlDag):
        return self.action(dag, *self.args, **self.kwargs)


def delayed(fn: Callable[Concatenate[DmlDag, _P], _T]) -> Callable[_P, _T]:
    def wrapped(*args: _P.args, **kwargs: _P.kwargs) -> _T:
        return DelayedAction(fn, *args, **kwargs)

    return wrapped


@delayed
def load(dag: DmlDag, name: str, key: str = "result") -> Node:
    return dag.load(name, key=key)


@delayed
def field(dag: DmlDag, default: Any = None, default_function: Optional[Callable] = None) -> _T:
    return default_function(dag) if default_function else default


@dataclass(init=False)
class Dag(AutoDataclassBase):
    dml: InitVar[Optional[Dml]] = None
    name: InitVar[Optional[str]] = None

    def __post_init__(self, dml: Optional[Dml], name: Optional[str]) -> None:
        dml = dml or Dml()
        deps, order = proc_deps(self.__class__)
        name = name or get_dag_name(self.__class__)
        message = self.__doc__ or f"Dag: {name}"
        dag = dml.new(name, message=message)
        for fld in [x.name for x in fields(self) if hasattr(self, x.name)]:
            obj = getattr(self, fld)
            if isinstance(obj, DelayedAction):
                obj = obj(dag)
            object.__setattr__(self, fld, dag.put(obj, name=fld))
        for method_name in order:
            method = getattr(self, method_name)
            if isinstance(method, DelayedAction):
                method = method(dag)
            # if isinstance(method, Node):
            #     method = method.value()
            if not isinstance(method, Executable):
                method = funk(method)
            assert isinstance(method, Executable)
            method.prepop.update({k: dag[k] for k in deps[method_name] if hasattr(dag, k)})
            dag.put(method, name=method_name)
            object.__setattr__(self, method_name, dag.put(method, name=method_name))
        self.dag = dag
        self.dml = dml

    def __init__(self, *args, **kwargs):
        # AutoDataclassBase will call __post_init__
        super().__init__(*args, **kwargs)

    def __enter__(self):
        self.dag.__enter__()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return self.dag.__exit__(exc_type, exc_value, traceback)

    def call(self, *args, **kwargs) -> Node:
        return self.dag.call(*args, **kwargs)

    def put(self, obj: Any, name: Optional[str] = None) -> Node:
        return self.dag.put(obj, name=name)

    def commit(self, obj: Any = None) -> None:
        return self.dag.commit(obj)
