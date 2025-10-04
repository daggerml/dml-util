from typing import Any

import pytest
from daggerml import Dml, Node

from dml_util.experimental import api
from dml_util.experimental.api import build_class_graph


def dec(fn):
    def wrapper(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapper


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


@pytest.mark.usefixtures("debug", "dml")
class TestDagCompilation:
    def test_dag_name_default(self):
        assert api.get_dag_name(MyDag) == "tests:unit:experimental:test_api::MyDag"

    def test_access_does_not_exist(self):
        class DagClass(MyDag):
            def step1(self):
                self.doesnotexist  # noqa: B018

        with pytest.raises(ValueError, match="depends on unknown field or method: 'doesnotexist'"):
            DagClass()

    def test_reserved_words(self):
        class DagClass(api.Dag):
            a: int = 5

            def fn(self, arg0):
                return self.commit(self.put(self.argv + self.a))

        deps, order = api.proc_deps(DagClass)
        assert list(order) == ["fn"]
        assert deps == {"fn": ["a"]}

    def test_reserved_words_defined(self):
        class DagClass(api.Dag):
            a: int = 5

            def argv(self):
                return 3

        with pytest.raises(ValueError, match="Field or method name 'argv' is reserved"):
            api.proc_deps(DagClass)

    def test_subclass(self):
        class DagClass(MyDag):
            foo: int = 5

            def step1(self, arg0, arg1):
                # Note `dag_arg`
                self.intermediate = arg0.value() * self.dag_arg.value()  # note `intermediate` is not a dep
                return self.step0(self.intermediate).value() + arg1.value() + 5

            def step2(self, arg0):
                return self.step1(arg0).value() * self.foo.value()

        deps, order = api.proc_deps(DagClass)
        assert list(order) == ["step0", "step1", "step2"]
        assert deps == {"step0": [], "step1": ["dag_arg", "step0"], "step2": ["foo", "step1"]}

    @pytest.mark.slow
    @pytest.mark.parametrize("val", [1, "asdf", True, None])
    def test_fields_default(self, val):
        class MyTestDag(api.Dag):
            a0: Any = api.field(default=val)

        assert isinstance(MyTestDag.a0, api.DelayedAction)

        dag = MyTestDag()
        assert dag.a0.value() == val

    @pytest.mark.slow
    def test_fields_default_function(self):
        class MyTestDag(api.Dag):
            a0: int = 0
            a1: Any = api.field(default=1)
            a2: Node = api.field(default_function=lambda dag: [dag.a0, dag.a1])

        assert isinstance(MyTestDag.a1, api.DelayedAction)

        dag = MyTestDag()
        assert dag.a2.value() == [0, 1]

    @pytest.mark.slow
    def test_load(self):
        class MyTestDag(api.Dag):
            a0: Node = api.load("d0")
            a1: Node = api.load("d0", key="other")

        assert isinstance(MyTestDag.a1, api.DelayedAction)
        with Dml().new("d0") as dag:
            dag.put(2, name="other")
            dag.commit(1)
        dag = MyTestDag()
        assert dag.a0.value() == 1
        assert dag.a1.value() == 2

    @pytest.mark.slow
    def test_context_manager(self):
        class MyTestDag(api.Dag):
            pass

        with pytest.raises(ZeroDivisionError, match="division by zero"):
            with MyTestDag() as dag:  # noqa: F841
                1 / 0  # noqa: B018
