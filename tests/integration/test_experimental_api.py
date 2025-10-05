from typing import Any

import daggerml.core
import pytest
from daggerml.core import Dml, Error, Node

from dml_util import funk
from dml_util.experimental import _api as api_impl
from dml_util.experimental import api

pytestmark = [pytest.mark.slow, pytest.mark.needs_dml]  # marks the entire file


@pytest.mark.usefixtures("dml")
class TestDagApi:
    def test_run_funk(self):
        class DagClass(api.Dag):
            arg: int = 2

            def inc(self, arg0):
                return arg0.value() + 1

            def step1(self, arg0, arg1):
                self.intermediate = self.inc(arg0.value()).value() * self.arg.value()
                return self.intermediate.value() + arg1.value()

        dag = DagClass()
        res = dag.step1(3, 5)
        assert res.value() == 13  # (3+1)*2 + 5
        assert res.load().intermediate.value() == 8

    def test_dag_message(self, dml):
        class MyDagWithDoc(api.Dag):
            """This is my custom dag"""

            def step0(self, arg0):
                return arg0.value() + 1

        my_dag = MyDagWithDoc()
        my_dag.dag.commit(2)
        assert dml("commit", "list")[0]["message"] == "This is my custom dag"

    def test_funk_docstring(self):
        class DagClass(api.Dag):
            foo: int = 5

            def step1(self, arg0, arg1):
                """This is my custom step1 function"""
                self.intermediate = arg0.value() * self.foo.value()
                return self.intermediate.value() + arg1.value()

            @api.funk(prepop={"x": 3})
            def step2(self, arg0, arg1):
                """This is my custom step2 function"""
                return arg0.value() + arg1.value() + self.x.value()

        dag = DagClass()
        assert isinstance(dag.step1, Node)
        desc = dag.dag.dml("node", "describe", dag.step1.ref.to)
        assert desc["doc"] == "This is my custom step1 function"
        assert isinstance(dag.step2, Node)
        desc = dag.dag.dml("node", "describe", dag.step2.ref.to)
        assert desc["doc"] == "This is my custom step2 function"

    def test_with_funks(self):
        class DagClass(api.Dag):
            dag_arg: int = 2

            @api.funk(prepop={"x": 3})
            def step1(self, arg0, arg1):
                self.intermediate = arg0.value() * self.dag_arg.value()
                return self.intermediate.value() + arg1.value() + 5

        my_dag = DagClass()
        assert isinstance(my_dag.dag, daggerml.core.Dag)
        assert isinstance(my_dag.dag_arg, daggerml.core.ScalarNode)
        assert my_dag.step1.value().prepop == {"dag_arg": 2, "x": 3}

    def test_prepop_precedence(self):
        class DagClass(api.Dag):
            foo: int = 2

            @api.funk(prepop={"foo": 3})
            def step1(self):
                self.foo  # noqa: B018

        dag = DagClass()  # does not raise
        assert dag.step1.value().prepop == {"foo": 3}

    def test_with_funks_n_loads(self, dml):
        dml.new("test").commit(5)

        class DagClass(api.Dag):
            arg: int = dml.load("test").result

            def fn(self, arg0):
                return self.arg.value() + arg0.value()

        my_dag = DagClass()
        assert my_dag.fn(2).value() == 7

    @pytest.mark.parametrize("val", [1, "asdf", True, None])
    def test_fields_default(self, val):
        class MyTestDag(api.Dag):
            a0: Any = api.field(default=val)

        assert isinstance(MyTestDag.a0, api_impl.DelayedAction)

        dag = MyTestDag()
        assert dag.a0.value() == val

    @pytest.mark.slow
    def test_fields_default_function(self):
        class MyTestDag(api.Dag):
            a0: int = 0
            a1: Any = api.field(default=1)
            a2: Node = api.field(default_function=lambda dag: [dag.a0, dag.a1])

        assert isinstance(MyTestDag.a1, api_impl.DelayedAction)

        dag = MyTestDag()
        assert dag.a2.value() == [0, 1]

    @pytest.mark.slow
    def test_load(self):
        class MyTestDag(api.Dag):
            a0: Node = api.load("d0")
            a1: Node = api.load("d0", key="other")

        assert isinstance(MyTestDag.a1, api_impl.DelayedAction)
        with pytest.raises(Error, match="no such dag: d0"):
            MyTestDag()
        with Dml().new("d0") as dag:
            dag.put(2, name="other")
            dag.commit(1)
        dag = MyTestDag()
        assert dag.a0.value() == 1
        assert dag.a1.value() == 2

    @pytest.mark.slow
    def test_load_in_funk(self):
        class MyTestDag(api.Dag):
            a0: Node = api.load("d0")
            a1: Node = api.load("d0", key="other")

            @api.funk(adapter=api.load("d0", key="other"))
            def get_other(self, x):
                return [self.a0, self.a1, x]

        assert isinstance(MyTestDag.a1, api_impl.DelayedAction)
        with Dml().new("d0") as dag:
            dag.put("local", name="other")
            dag.commit(1)
        dag = MyTestDag()
        assert dag.a0.value() == 1
        assert dag.a1.value() == "local"

    @pytest.mark.slow
    def test_field_in_funk(self):
        class MyTestDag(api.Dag):
            a0: Node = api.load("d0")
            a1: Node = api.load("d0", key="other")

            @api.funk(adapter=api.field(default_function=lambda dag: dag.a1))
            def get_other(self, x):
                return [self.a0, self.a1, x]

        assert isinstance(MyTestDag.a1, api_impl.DelayedAction)
        with Dml().new("d0") as dag:
            dag.put("local", name="other")
            dag.commit(1)
        dag = MyTestDag()
        assert dag.a0.value() == 1
        assert dag.a1.value() == "local"

    @pytest.mark.slow
    def test_ref_fails(self):
        class MyTestDag(api.Dag):
            a0: int = 1
            a1: Node = api.ref.a2
            a2: Node = api.ref("a0")

        assert isinstance(MyTestDag.a1, api_impl.DelayedAction)
        with pytest.raises(Error, match="Key a2 not in "):
            MyTestDag()

    @pytest.mark.slow
    def test_ref(self):
        class MyTestDag(api.Dag):
            a0: int = 1
            a2: Node = api.ref("a0")  # names are not lexicographically sorted
            a1: Node = api.ref.a2

        assert isinstance(MyTestDag.a1, api_impl.DelayedAction)
        dag = MyTestDag()
        assert dag.a0.value() == 1
        assert dag.a1.value() == 1
        assert dag.a2.value() == 1

    @pytest.mark.slow
    def test_ref_in_fn(self):
        class MyTestDag(api.Dag):
            a0: int = 1

            @api.funk(prepop={"x": api.ref.a0})
            def get_a2(self):
                return self.x

        dag = MyTestDag()
        assert dag.get_a2().value() == 1

    @pytest.mark.slow
    def test_context_manager(self):
        class MyTestDag(api.Dag):
            pass

        with pytest.raises(ZeroDivisionError, match="division by zero"):
            with MyTestDag() as dag:  # noqa: F841
                1 / 0  # noqa: B018

    def get_tarball(self, dml):
        from contextlib import redirect_stderr, redirect_stdout

        from dml_util import S3Store
        from tests.util import _root_

        s3 = S3Store()
        excludes = [
            "tests/*.py",
            ".pytest_cache",
            ".ruff_cache",
            "**/__about__.py",
            "__pycache__",
            "examples",
            ".venv",
            "**/.venv",
        ]
        with redirect_stdout(None), redirect_stderr(None):
            return s3.tar(dml, str(_root_), excludes=excludes)

    @pytest.mark.usefixtures("s3_bucket", "logs", "debug")
    def test_docker(self, docker_flags):
        class A0(api.Dag):
            dkr_build: Any = api.field(default=funk.dkr_build)
            dkr_flags: Any = api.field(default=docker_flags)
            tarball: Any = api.field(default_function=lambda dag: self.get_tarball(dag.dml))

            @api.funk(prepop={"x": 1})
            def fn(self, *args):
                return sum([x.value() for x in args]) + self.x.value()

            def dockerify(self, tarball):
                img = self.dkr_build(
                    tarball,
                    ["--platform", "linux/amd64", "-f", "tests/assets/dkr-context/Dockerfile"],
                    timeout=60_000,
                    name="img",
                )
                fn = self.wrap(
                    self.fn,
                    {
                        "uri": "docker",
                        "data": {"image": img, "flags": self.dkr_flags},
                        "adapter": "dml-util-local-adapter",
                    },
                )
                return fn

            def run(self, *vals):
                fn = self.dockerify(self.tarball, name="dockerified-fn")
                baz = fn(*vals, name="baz")
                return baz

        with A0() as dag:
            vals = [1, 2, 3, 4, 5]
            resp = dag.run(*vals)
            assert resp.value() == sum(vals) + 1
