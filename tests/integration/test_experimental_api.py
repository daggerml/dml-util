import daggerml.core
import pytest

from dml_util.experimental import api

pytestmark = pytest.mark.slow  # marks the entire file as slow for pytest.


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
