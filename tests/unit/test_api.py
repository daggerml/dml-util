import os
from dataclasses import is_dataclass, replace
from tempfile import TemporaryDirectory
from unittest.mock import patch

import daggerml.core
import pytest
from daggerml import Dml
from daggerml.core import ExecutableNode, ScalarNode

from dml_util import api
from dml_util.api import build_class_graph


def dec(fn):
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapper


@pytest.fixture(autouse=True)
def dml():
    with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
        with Dml.temporary(cache_path=tmpd) as _dml:
            with patch.dict(os.environ, {"DML_CACHE_PATH": tmpd, **_dml.envvars}):
                yield _dml


class BasicAccess:
    const = 3

    def m2(self, x):
        return x

    def return_value(self):
        return 42

    def define_first(self, x):
        self.foo = 12  # should not count as dependency
        return self.foo + x

    def use_internal(self, x):
        return self.m2(x) + self.const

    def define_fn_with_deps_inside(self, x):
        def inner_fn(y):
            return self.m2(y)

        return inner_fn(x) + self.const

    def define_fn_with_deps_inside_no_use(self, x):
        def inner_fn(y):
            return self.m2(y)

        return self.const + x

    def recursive_use_internal(self, x):
        return self.use_internal(x - 1)

    @dec
    def return_value_decorated(self):
        return 42

    @dec
    def define_first_decorated(self, x):
        self.foo = 12
        return self.foo + x

    @dec
    def use_internal_decorated(self, x):
        return self.m2(x) + self.const


class TestAnalyzer:
    def test_m2(self):
        graph = build_class_graph(BasicAccess)
        assert graph["m2"] == {"internal": []}

    def test_return_value(self):
        graph = build_class_graph(BasicAccess)
        assert graph["return_value"] == {"internal": []}

    def test_define_first(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_first"] == {"internal": []}

    def test_use_internal(self):
        graph = build_class_graph(BasicAccess)
        assert graph["use_internal"] == {"internal": ["const", "m2"]}

    def test_define_fn_with_deps_inside(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_fn_with_deps_inside"] == {"internal": ["const", "m2"]}

    def test_define_fn_with_deps_inside_no_use(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_fn_with_deps_inside_no_use"] == {"internal": ["const", "m2"]}

    def test_recursive_use_internal(self):
        graph = build_class_graph(BasicAccess)
        assert graph["recursive_use_internal"] == {"internal": ["use_internal"]}

    def test_return_value_decorated(self):
        graph = build_class_graph(BasicAccess)
        assert graph["return_value_decorated"] == {"internal": []}

    def test_define_first_decorated(self):
        graph = build_class_graph(BasicAccess)
        assert graph["define_first_decorated"] == {"internal": []}

    def test_use_internal_decorated(self):
        graph = build_class_graph(BasicAccess)
        assert graph["use_internal_decorated"] == {"internal": ["const", "m2"]}

    def test_subclasses(self):
        class SubClass(BasicAccess):
            def m3(self, x):
                return self.m2(x) + self.const

        graph = build_class_graph(SubClass)
        assert graph["m3"] == {"internal": ["const", "m2"]}
        assert set(graph.keys()) == set(build_class_graph(BasicAccess).keys()) | {"m3"}

    def test_subclasses_override(self):
        class SubClass(BasicAccess):
            foo: int = 5

            def use_internal(self, x):
                return self.const * x.value() + self.foo

        graph = build_class_graph(SubClass)
        assert graph["use_internal"] == {"internal": ["const", "foo"]}
        assert set(graph.keys()) == set(build_class_graph(BasicAccess).keys())


class MyDag(api.Dag):
    dag_arg: int = 10

    def step0(self, arg0):
        return arg0.value() + 1


class TestDag:
    def test_dag_name_default(self, dml):
        my_dag = MyDag()
        my_dag.dag.commit(2)
        assert [x["name"] for x in dml("dag", "list")] == ["tests:unit:test_api::MyDag"]

    def test_dag_name_explicit(self, dml):
        my_dag = MyDag(name="custom_name")
        my_dag.dag.commit(2)
        assert [x["name"] for x in dml("dag", "list")] == ["custom_name"]

    def test_dml_instance(self, dml):
        my_dag = MyDag(dml=dml)
        assert replace(my_dag.dag.dml, token=None) == dml

    def test_node_attrs(self):
        my_dag = MyDag()
        assert is_dataclass(my_dag)
        assert isinstance(my_dag.dag, daggerml.core.Dag)
        assert isinstance(my_dag.dag_arg, ScalarNode)
        assert isinstance(my_dag.step0, ExecutableNode)

    def test_with_funks(self):
        class DagClass(api.Dag):
            dag_arg: int = 2

            @api.funk(prepop={"x": 3})
            def step1(self, arg0, arg1):
                self.intermediate = arg0.value() * self.dag_arg.value()
                return self.intermediate.value() + arg1.value() + 5

        my_dag = DagClass()
        assert is_dataclass(my_dag)
        assert isinstance(my_dag.dag, daggerml.core.Dag)
        assert isinstance(my_dag.dag_arg, ScalarNode)
        assert my_dag.step1.value().prepop == {"dag_arg": 2, "x": 3}

    def test_subclass(self):
        class DagClass(MyDag):
            foo: int = 5

            def step1(self, arg0, arg1):
                # Note `dag_arg`
                self.intermediate = arg0.value() * self.dag_arg.value()
                return self.intermediate.value() + arg1.value() + 5

        dag = DagClass()
        assert is_dataclass(dag)
        assert isinstance(dag.dag, daggerml.core.Dag)
        assert isinstance(dag.foo, ScalarNode)
        assert isinstance(dag.step1, ExecutableNode)
        # check inherited fields and meethods
        assert isinstance(dag.dag_arg, ScalarNode)
        assert isinstance(dag.step0, ExecutableNode)


def test_run_funk():
    class DagClass(MyDag):
        def step1(self, arg0, arg1):
            self.intermediate = arg0.value() * self.dag_arg.value()
            return self.intermediate.value() + arg1.value() + 5

    dag = DagClass()
    assert dag.step1.value().prepop == {"dag_arg": 10}
    assert dag.step1(1, 2).value() == 17
