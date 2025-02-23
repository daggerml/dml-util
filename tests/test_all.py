import os
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase, mock

from daggerml.core import Dml, Error, Node

from dml_util import DOCKER_EXEC, funkify

_root_ = Path(__file__).parent.parent


class TestBasic(TestCase):
    def test_funkify(self):
        def fn(*args):
            return sum(args)

        @funkify(extra_fns=[fn])
        def dag_fn(dag):
            dag.result = fn(*dag.argv[1:].value())
            return dag.result

        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with Dml() as dml:
                    vals = [1, 2, 3]
                    d0 = dml.new("d0", "d0")
                    d0.f0 = dag_fn
                    d0.n0 = d0.f0(*vals)
                    assert d0.n0.value() == sum(vals)
                    # you can get the original back
                    d0.f1 = funkify(dag_fn.fn, extra_fns=[fn])
                    d0.n1 = d0.f1(*vals)
                    assert d0.n1.value() == sum(vals)
            # ensure files created
            cache_dir = f"{fn_cache_dir}/cache/dml-util/"
            assert len(os.listdir(cache_dir)) == 1
            (fnid,) = os.listdir(cache_dir)
            self.assertCountEqual(
                os.listdir(f"{cache_dir}/{fnid}/"),
                ["stdout", "stderr", "input.dump", "output.dump", "script"],
            )

    def test_funkify_errors(self):
        @funkify
        def dag_fn(dag):
            dag.result = sum(dag.argv[1:].value()) / 0
            return dag.result

        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with Dml() as dml:
                    d0 = dml.new("d0", "d0")
                    d0.f0 = dag_fn
                    with self.assertRaises(Error):
                        d0.n0 = d0.f0(1, 2, 3)

    def test_monkey_patch(self):
        @funkify
        def dag_fn(dag):
            dag.result = sum(dag.argv[1:].value())
            return dag.result

        vals = [1, 2, 3]
        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with Dml() as dml:
                    d0 = dml.new("d0", "d0")
                    val = d0(DOCKER_EXEC)
                    assert isinstance(val, Node)
                    assert val.value() == DOCKER_EXEC
                    d0.n0 = d0(dag_fn)(*vals)
                    assert d0.n0.value() == sum(vals)
