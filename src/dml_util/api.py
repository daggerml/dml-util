import json
import os
import re
import subprocess
from dataclasses import dataclass, fields, is_dataclass
from functools import partial, wraps
from inspect import getsource
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Callable, Concatenate, Dict, Optional, ParamSpec, Sequence, Type, Union, overload

from daggerml import Dml, Executable

from dml_util.lib.analyzer import build_class_graph, get_dag_name, topo_sort

if TYPE_CHECKING:
    import daggerml.core

_P = ParamSpec("_P")

@overload
def funk(
    fn: None = None,
    *,
    prepop: Optional[Dict[str, Any]] = None,
    extra_fns: Sequence[Callable] = (),
) -> Callable[[Callable[Concatenate["daggerml.core.Dag", _P], Any]], Executable]: ...
@overload
def funk(
    fn: Callable[Concatenate["daggerml.core.Dag", _P], Any],
    *,
    prepop: Optional[Dict[str, Any]] = None,
    extra_fns: Sequence[Callable] = (),
) -> Executable: ...
def funk(
    fn: Optional[Callable] = None,
    *,
    prepop: Optional[Dict[str, Any]] = None,
    extra_fns: Sequence[Callable] = (),
) -> Union[Executable, Callable]:
    """A decorator that marks a method as a funk (a Dag step)."""
    if fn is None:
        return partial(funk, prepop=prepop, extra_fns=extra_fns)
    def get_src(f):
        lines = dedent(getsource(f)).split("\n")
        while not lines[0].startswith("def "):
            lines.pop(0)
        return "\n".join(lines)
    tpl = dedent(
        """
        #!/usr/bin/env python3
        from dml_util import aws_fndag

        {src}

        if __name__ == "__main__":
            with aws_fndag() as dag:
                res = {fn_name}(dag, *dag.argv[1:])
                if dag.ref is None:
                    dag.commit(res)
        """
    ).strip()
    src = tpl.format(
        src="\n\n".join([get_src(f) for f in [*extra_fns, fn]]),
        fn_name=fn.__name__,
    )
    src = re.sub(r"\n{3,}", "\n\n", src)
    return Executable(
        "script",
        data={"script": src},
        prepop=prepop or {},
        adapter="dml-util-local-adapter",
    )


def compile(cls: Type, dml: Optional[Dml] = None) -> type:
    """Compiles a Dag class and runs the Dag.run() method.

    Assumptions:
    1. the dag is defined in a file in the dags/ directory.
    2. the dag is a dataclass with no-arg constructor.
    """
    assert is_dataclass(cls), "Dag must be a dataclass"
    dependencies = build_class_graph(cls)
    order = topo_sort({k: [x for x in v["internal"] if x in dependencies.keys()] for k, v in dependencies.items()})
    dag_name = get_dag_name(cls)
    dag_instance = cls()
    dml = (dml or Dml())
    with dml.new(dag_name) as dag:
        for field in fields(dag_instance):
            dag[field.name] = getattr(dag_instance, field.name)
        for method_name in order:
            method = getattr(dag_instance, method_name)
            if not isinstance(method, Executable):
                method = funk(method)
            assert isinstance(method, Executable)
            method["prepop"].update({k: dag[k] for k in dependencies[method_name]["internal"]})
            dag[method_name] = method
            setattr(dag_instance, method_name, method)
    return cls

@overload
def dag(cls: None = None, *, dml: Optional[Dml] = None) -> Callable[[Type], Type]: ...
@overload
def dag(cls: Type, dml: Optional[Dml] = None) -> Type: ...
def dag(cls: Optional[Type] = None, dml: Optional[Dml] = None) -> Union[Type, Callable[[Type], Type]]:
    """A decorator that marks a class as a Dag.

    On instantiation, the Dag class is compiled and then the user can call whatever
    methods they want on the instance as desired.

    Assumptions:
    1. the dag is defined in a file in the dags/ directory.
    2. the dag is a dataclass with no-arg constructor.
    3. The user will call whatever methods they want on the instance as desired.

    Notes:
    - This decorator creates a dataclass instance.
    - The user can use dataclass style stuff with fields and whatnot.

    Example
    -------
    >>> from dml_util import api, funkify
    >>> @api.dag
    >>> class MyDag:
    >>>     dag_arg: int = 10
    >>>     def step0(self, arg0):
    >>>         return arg0 + 1
    >>>     @funkify(uri="batch")
    >>>     @api.funk  # => funkify(prepop={"dag_arg": self.dag_arg})
    >>>     def step1(self, arg0, arg1):
    >>>         import pandas as pd
    >>>         df = pd.read_parquet(arg0.value().uri)
    >>>         self.intermediate = arg0.value() * self.dag_arg.value()
    >>>         return self.intermediate.value() + arg1.value() + 5
    >>>     def step2(self, arg0, arg1, step1):
    >>>         step1 = self.step0(step1)
    >>>         return step1.value() * 2 + self.step1(arg0, arg1)
    >>>     def run(self):
    >>>         result1 = self.step1(1, 2)
    >>>         result2 = self.step2(6, 7, result1)
    >>>         print(f"Step1 Result: {result1}, Step2 Result: {result2}")
    >>> my_dag = MyDag()
    >>> from dataclasses import is_dataclass
    >>> assert is_dataclass(my_dag)  # MyDag is a dataclass instance
    >>> assert isinstance(my_dag.step0, Executable)  # step0 is now an Executable
    >>> assert isinstance(my_dag.step1, Executable)  # step1 is now an Executable
    >>> assert isinstance(my_dag.step2, Executable)  # step2 is now an Executable
    >>> assert isinstance(my_dag.run, Executable)  # run is now an Executable
    >>> assert my_dag.run.prepop == {"step1": my_dag.step1, "step2": my_dag.step2}
    """
    if cls is None:
        return partial(dag, dml=dml)
    # Ensure the class is a dataclass so fields() works and matches docstring expectations
    _cls = cls if is_dataclass(cls) else dataclass(cls)
    dml = dml or Dml()

    @wraps(cls)
    def wrapper(*args, **kwargs):
        instance = _cls(*args, **kwargs)
        assert is_dataclass(instance), "Dag must be a dataclass"
        dependencies = build_class_graph(cls)
        order = reversed(topo_sort({k: [x for x in v["internal"] if x in dependencies.keys()] for k, v in dependencies.items()}))
        print(order)
        dag_name = get_dag_name(cls)
        with dml.new(dag_name) as dag:
            for field in fields(instance):
                dag[field.name] = getattr(instance, field.name)
            for method_name in order:
                method = getattr(instance, method_name)
                if not isinstance(method, Executable):
                    method = funk(method)
                assert isinstance(method, Executable)
                method.prepop.update({k: dag[k] for k in dependencies[method_name]["internal"]})
                dag[method_name] = method
                setattr(instance, method_name, dag[method_name])
        return instance
    return wrapper
