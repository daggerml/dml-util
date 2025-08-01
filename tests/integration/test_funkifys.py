import os
import re
import shutil
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr, redirect_stdout
from tempfile import TemporaryDirectory
from unittest import skipIf, skipUnless
from unittest.mock import patch

import boto3
import pytest
from daggerml import Dml, Error, Resource

import dml_util.wrapper  # noqa: F401
from dml_util import funk
from dml_util.aws.s3 import S3Store
from dml_util.funk import funkify
from tests.util import _root_

pytestmark = pytest.mark.slow  # marks the entire file as slow for pytest.
VALID_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+")


class TestMisc:
    def test_git_info(self):
        with Dml.temporary() as dml:
            d0 = dml.new("d0", "d0")
            git_info = d0[".dml/git"].value()
            assert isinstance(git_info, dict)
            assert set(git_info) == {"branch", "commit", "remote", "status"}
            assert all(type(x) is str for x in git_info.values())

    def test_subdag_caching(self):
        @funkify
        def subdag_fn(dag):
            from uuid import uuid4

            return uuid4().hex

        @funkify
        def dag_fn(dag):
            from uuid import uuid4

            fn, *args = dag.argv[1:]
            return {str(x.value()): fn(x) for x in args}, uuid4().hex

        vals = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                d0 = dml.new("d0", "d0")
                d0.dag_fn = dag_fn
                d0.subdag_fn = subdag_fn
                with ThreadPoolExecutor(2) as pool:
                    futs = [pool.submit(d0.dag_fn, d0.subdag_fn, *args) for args in [vals, reversed(vals)]]
                    a, b = [f.result() for f in futs]
                assert a != b
                assert a[0].value() == b[0].value()
                assert a[1].value() != b[1].value()


class TestFunkSingles:
    @pytest.mark.usefixtures("s3_bucket", "logs")
    def test_script(self):
        @funkify
        def dag_fn(dag):
            import sys  # noqa: F811

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())
            return dag.result

        client = boto3.client("logs")
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                vals = [1, 2, 3]
                with dml.new("d0", "d0") as d0:
                    d0.f0 = dag_fn
                    node = d0.f0(*vals)
                    dag = node.load()
                config = dag[".dml/env"].value()
        logs = client.get_log_events(logGroupName=config["log_group"], logStreamName=config["log_stdout"])["events"]
        assert [
            f"*** Starting {config['run_id']} ***",
            "testing stdout...",
            f"*** Ending {config['run_id']} ***",
        ] == [x["message"] for x in logs]
        logs = client.get_log_events(logGroupName=config["log_group"], logStreamName=config["log_stderr"])["events"]
        assert "testing stderr..." in {x["message"] for x in logs}

    @pytest.mark.usefixtures("s3_bucket", "logs")
    def test_script_errors(self):
        @funkify
        def dag_fn(dag):
            dag.result = dag.argv[1].value() / dag.argv[-1].value()
            return dag.result

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                d0 = dml.new("d0", "d0")
                d0.f0 = dag_fn
                with pytest.raises(Error) as exc:
                    d0.n0 = d0.f0(1, 0)
                assert "division by zero" in str(exc.value)

    @skipUnless(shutil.which("hatch"), "hatch is not available")
    @pytest.mark.usefixtures("s3_bucket", "logs")
    def test_hatch(self):
        @funkify(uri="hatch", data={"name": "pandas", "path": str(_root_)})
        @funkify
        def dag_fn(dag):
            import pandas as pd

            print("testing stdout...")
            return pd.__version__

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                d0 = dml.new("d0", "d0")
                d0.f0 = dag_fn
                result = d0.f0()
                assert VALID_VERSION.match(result.value())
                config = result.load()[".dml/env"].value()
        client = boto3.client("logs")
        logs = client.get_log_events(logGroupName=config["log_group"], logStreamName=config["log_stdout"])["events"]
        assert len(logs) == 3
        assert logs[1]["message"] == "testing stdout..."

    @skipIf(not shutil.which("conda"), "conda is not available")
    @pytest.mark.usefixtures("s3_bucket", "logs")
    def test_conda(self):
        with pytest.raises(ModuleNotFoundError) as exc:
            import pandas  # noqa: F401
        assert "No module named 'pandas'" in str(exc.value)

        @funkify(
            uri="conda",
            data={"name": "dml-pandas"},
        )
        @funkify
        def dag_fn(dag):
            import pandas as pd

            print("testing stdout...")
            return pd.__version__

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                d0 = dml.new("d0", "d0")
                d0.f0 = dag_fn
                result = d0.f0()
                assert VALID_VERSION.match(result.value())
                config = result.load()[".dml/env"].value()
        client = boto3.client("logs")
        logs = client.get_log_events(logGroupName=config["log_group"], logStreamName=config["log_stdout"])["events"]
        assert len(logs) == 3
        assert logs[1]["message"] == "testing stdout..."

    @skipUnless(shutil.which("ssh"), "ssh is not available")
    @skipUnless(shutil.which("hatch"), "hatch is not available")
    def test_ssh(self, ssh_resource_data):
        @funkify(uri="ssh", data=ssh_resource_data)
        @funkify(uri="hatch", data={"name": "pandas", "path": str(_root_)})
        @funkify
        def fn(dag):
            return sum(dag.argv[1:].value())

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                with dml.new("test", "asdf") as dag:
                    dag.fn = fn
                    res = dag.fn(1, 2, 3)
                    assert res.value() == 6

    @skipUnless(shutil.which("ssh"), "ssh is not available")
    @skipUnless(shutil.which("hatch"), "hatch is not available")
    def test_ssh_error(self, ssh_resource_data):
        @funkify(uri="ssh", data=ssh_resource_data)
        @funkify(uri="hatch", data={"name": "pandas", "path": str(_root_)})
        @funkify
        def fn(dag):
            *nums, divisor = dag.argv[1:].value()
            return sum(nums) / divisor

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                with dml.new("test", "asdf") as dag:
                    dag.fn = fn
                    with pytest.raises(Error) as exc:
                        # This should raise a division by zero error
                        dag.n0 = dag.fn(1, 2, 3, 0)
                    assert "division by zero" in str(exc.value)

    @skipUnless(shutil.which("ssh"), "ssh is not available")
    def test_ssh_adapter_error(self, ssh_resource_data):
        old_path = os.environ["PATH"]
        with patch.dict(os.environ, {"PATH": f"{_root_}/tests/assets:{old_path}"}):

            @funkify(uri="ssh", data=ssh_resource_data)
            @funkify(uri="bogus", adapter="error-adapter.py")
            @funkify
            def fn(dag):
                return

            with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
                with Dml.temporary(cache_path=tmpd) as dml:
                    dag = dml.new("test", "asdf")
                    dag.fn = fn
                    with pytest.raises(Error) as exc:
                        dag.n0 = dag.fn(1, 2, 3)
                    assert "Simulated adapter error" in str(exc.value)

    @pytest.mark.usefixtures("s3_bucket")
    def test_notebooks(self):
        s3 = S3Store()
        vals = [1, 2, 3, 4]
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                dag = dml.new("bar")
                dag.nb = s3.put(filepath=_root_ / "tests/assets/notebook.ipynb", suffix=".ipynb")
                dag.nb_exec = funk.execute_notebook
                dag.html = dag.nb_exec(dag.nb, *vals)
                dag.result = dag.html
                html = s3.get(dag.result).decode().strip()
                assert html.startswith("<!DOCTYPE html>")
                assert f"Total sum = {sum(vals)}" in html

    @pytest.mark.usefixtures("s3_bucket")
    def test_cfn(self):
        tpl = {
            "AWSTemplateFormatVersion": "2010-09-09",
            "Description": "A simple CloudFormation template that creates an S3 bucket.",
            "Resources": {
                "MyS3Bucket": {
                    "Type": "AWS::S3::Bucket",
                    "Properties": {"BucketName": "my-simple-bucket-123456"},
                }
            },
            "Outputs": {
                "BucketName": {
                    "Description": "The name of the created S3 bucket",
                    "Value": {"Ref": "MyS3Bucket"},
                },
                "BucketArn": {
                    "Description": "The ARN of the created S3 bucket",
                    "Value": {"Fn::GetAtt": ["MyS3Bucket", "Arn"]},
                },
            },
        }
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                dag = dml.new("foo")
                dag.cfn = Resource("cfn", adapter="dml-util-local-adapter")
                dag.stack = dag.cfn("stacker", tpl, {})
                assert set(dag.stack.keys()) == {"BucketName", "BucketArn"}

    @pytest.mark.usefixtures("s3_bucket", "logs", "debug")
    def test_docker(self, docker_flags):
        @funkify
        def fn(dag):
            import sys  # noqa: F811

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())

        s3 = S3Store()
        vals = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml, patch.dict(os.environ, {"DML_FN_CACHE_DIR": tmpd}):
                dag = dml.new("test", "asdf")
                excludes = [
                    "tests/*.py",
                    ".pytest_cache",
                    ".ruff_cache",
                    "__pycache__",
                    "examples",
                    ".venv",
                    "**/.venv",
                ]
                with redirect_stdout(None), redirect_stderr(None):
                    dag.tar = s3.tar(dml, str(_root_), excludes=excludes)
                dag.dkr = funk.dkr_build
                dag.img = dag.dkr(
                    dag.tar,
                    ["--platform", "linux/amd64", "-f", "tests/assets/dkr-context/Dockerfile"],
                    timeout=60_000,
                )
                dag.fn = funkify(
                    fn,
                    "docker",
                    {"image": dag.img.value(), "flags": docker_flags},
                    adapter="local",
                )
                dag.baz = dag.fn(*vals)
                assert dag.baz.value() == sum(vals)
                dag2 = dml.load(dag.baz)
                assert dag2.result is not None
                dag2 = dml("dag", "describe", dag2._ref.to)


class TestFunkCombos:
    @skipUnless(shutil.which("conda"), "conda is not available")
    @skipUnless(shutil.which("hatch"), "hatch is not available")
    @pytest.mark.usefixtures("s3_bucket", "logs")
    def test_conda_in_hatch(self):
        with pytest.raises(ModuleNotFoundError) as exc:
            import pandas  # noqa: F401
        assert "No module named 'pandas'" in str(exc.value)

        @funkify(uri="conda", data={"name": "dml-pandas"})
        @funkify
        def dag_fn(dag):
            import pandas as pd

            print("stdout from inner func")
            return pd.__version__

        @funkify(uri="hatch", data={"name": "default", "path": str(_root_)})
        @funkify
        def dag_fn2(dag):
            print("stdout from outer func")
            try:
                import pandas  # noqa: F401

                raise RuntimeError("pandas should not be available")
            except ImportError:
                pass
            fn = dag.argv[1]
            return fn(name="fn")

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                dag = dml.new("d0", "d0")
                dag.dag_fn = dag_fn
                dag.dag_fn2 = dag_fn2
                result = dag.dag_fn2(dag.dag_fn)
                assert VALID_VERSION.match(result.value())
                config = result.load()[".dml/env"].value()
        client = boto3.client("logs")
        logs = client.get_log_events(logGroupName=config["log_group"], logStreamName=config["log_stdout"])["events"]
        assert len(logs) == 3
        assert logs[1]["message"] == "stdout from outer func"

    @skipUnless(shutil.which("conda"), "conda is not available")
    @skipUnless(shutil.which("hatch"), "hatch is not available")
    @pytest.mark.usefixtures("s3_bucket", "logs")
    def test_hatch_in_conda(self):
        with pytest.raises(ModuleNotFoundError) as exc:
            import polars  # noqa: F401
        assert "No module named 'polars'" in str(exc.value)

        @funkify(uri="hatch", data={"name": "polars", "path": str(_root_)})
        @funkify
        def dag_fn(dag):
            import polars as pl

            return pl.__version__

        @funkify(uri="conda", data={"name": "dml-pandas"})
        @funkify
        def dag_fn2(dag):
            try:
                import polars  # noqa: F401

                raise RuntimeError("polars should not be available")
            except ImportError:
                fn = dag.argv[1]
                return fn(*dag.argv[2:], name="fn")

        vals = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                dag = dml.new("d0", "d0")
                dag.dag_fn = dag_fn
                dag.dag_fn2 = dag_fn2
                result = dag.dag_fn2(dag.dag_fn, *vals).value()
                assert VALID_VERSION.match(result)

    @skipUnless(shutil.which("ssh"), "ssh is not available")
    @skipUnless(shutil.which("hatch"), "hatch is not available")
    def test_ssh_hatch(self, ssh_resource_data):
        @funkify(uri="ssh", data=ssh_resource_data)
        @funkify(uri="hatch", data={"name": "pandas", "path": str(_root_)})
        @funkify
        def fn(dag):
            import sys  # noqa: F811

            import pandas as pd

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            return pd.__version__

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                with dml.new("test", "asdf") as dag:
                    dag.fn = fn
                    res = dag.fn()
                    assert VALID_VERSION.match(res.value())
                    dag2 = dml.load(res)
                    assert dag2.result is not None
                dag = dml("dag", "describe", dag2._ref.to)

    @skipUnless(shutil.which("ssh"), "ssh is not available")
    @skipUnless(shutil.which("docker"), "docker not available")
    @skipUnless(shutil.which("hatch"), "hatch not available")
    @pytest.mark.usefixtures("s3_bucket", "logs")
    def test_ssh_hatch_docker(self, ssh_resource_data, docker_flags):
        @funkify
        def fn(dag):
            import sys  # noqa: F811

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())

        dkr_build_in_hatch = funkify(funk.dkr_build, "hatch", data={"name": "default", "path": str(_root_)})
        s3 = S3Store()
        vals = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml, patch.dict(os.environ, {"DML_FN_CACHE_DIR": tmpd}):
                excludes = [
                    "tests/*.py",
                    ".pytest_cache",
                    ".ruff_cache",
                    "__pycache__",
                    "examples",
                    ".venv",
                    "**/.venv",
                ]
                dag = dml.new("test", "asdf")
                dag.tar = s3.tar(dml, _root_, excludes=excludes)
                dag.dkr = funkify(dkr_build_in_hatch, uri="ssh", data=ssh_resource_data)
                dag.img = dag.dkr(
                    dag.tar,
                    ["--platform", "linux/amd64", "-f", "tests/assets/dkr-context/Dockerfile"],
                    timeout=60_000,
                )
                dag.fn = funkify(
                    funkify(
                        funkify(
                            fn,
                            "docker",
                            {"image": dag.img.value(), "flags": docker_flags},
                            adapter="local",
                        ),
                        uri="hatch",
                        data={"name": "default", "path": str(_root_)},
                    ),
                    uri="ssh",
                    data=ssh_resource_data,
                )
                dag.baz = dag.fn(*vals)
                assert dag.baz.value() == sum(vals)
                dag2 = dml.load(dag.baz)
                assert dag2.result is not None
                dag2 = dml("dag", "describe", dag2._ref.to)
