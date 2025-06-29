import getpass
import json
import os
import re
import shlex
import shutil
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from textwrap import dedent
from unittest import skipIf
from unittest.mock import patch

import boto3
from daggerml import Dml, Resource
from daggerml.core import Error

import dml_util.adapter as adapter
import dml_util.wrapper  # noqa: F401
from dml_util import S3Store, funk, funkify
from dml_util.lib.dkr import Ecr
from dml_util.runner import DockerRunner, HatchRunner
from tests.test_baseutil import S3_BUCKET, S3_PREFIX, AwsTestCase

_root_ = Path(__file__).parent.parent
VALID_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+")


class Config:
    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def __getattr__(self, item):
        return self.__dict__.get(item, None)


def tmpdir():
    return TemporaryDirectory(prefix="dml-util-test-")


class FullDmlTestCase(AwsTestCase):
    def setUp(self):
        super().setUp()
        boto3.client("logs", endpoint_url=self.moto_endpoint).create_log_group(logGroupName="dml")
        boto3.client("s3", endpoint_url=self.moto_endpoint).create_bucket(Bucket=S3_BUCKET)
        self.tmpd = tmpdir()
        os.environ["DML_FN_CACHE_DIR"] = self.tmpd.name
        # os.environ["DML_DEBUG"] = "1"

    def tearDown(self):
        s3 = S3Store()
        s3.rm(*s3.ls(recursive=True))
        self.tmpd.cleanup()
        logc = boto3.client("logs", endpoint_url=self.moto_endpoint)
        for log_stream in logc.describe_log_streams(logGroupName="dml")["logStreams"]:
            logc.delete_log_stream(logGroupName="dml", logStreamName=log_stream["logStreamName"])
        logc.delete_log_group(logGroupName="dml")
        super().tearDown()


class TestTooling(FullDmlTestCase):
    def test_s3_uri(self):
        s3 = S3Store()
        raw = b"foo bar baz"
        resp = s3.put(raw, name="foo.txt")
        assert resp.uri == f"s3://{S3_BUCKET}/{S3_PREFIX}/foo.txt"
        resp = s3.put(raw, uri=f"s3://{S3_BUCKET}/asdf/foo.txt")
        assert resp.uri == f"s3://{S3_BUCKET}/asdf/foo.txt"

    def test_runner(self):
        os.environ["DML_CACHE_KEY"] = "test_key"
        with tmpdir() as tmpd:
            conf = Config(
                uri="asdf:uri",
                input=f"{tmpd}/input.dump",
                output=f"{tmpd}/output.dump",
                error=f"{tmpd}/error.dump",
                n_iters=1,
            )
            with open(conf.input, "w") as f:
                f.write("foo")
            with patch.object(adapter.Adapter, "send_to_remote", return_value=(None, "testing0")):
                status = adapter.Adapter.cli(conf)
                assert status == 0
                assert not os.path.exists(conf.output)
                with open(conf.error, "r+") as f:
                    assert f.read().strip() == "testing0"
                os.truncate(conf.error, 0)
            with patch.object(
                adapter.Adapter,
                "send_to_remote",
                return_value=("my-dump", "testing1"),
            ):
                status = adapter.Adapter.cli(conf)
                assert status == 0
                with open(conf.output, "r") as f:
                    assert f.read() == "my-dump"
                os.truncate(conf.output, 0)
                with open(conf.error, "r") as f:
                    assert f.read().strip() == "testing1"
                os.truncate(conf.error, 0)

    def test_runner_daemon(self):
        os.environ["DML_CACHE_KEY"] = "test_key"
        with tmpdir() as tmpd:
            conf = Config(
                uri="asdf:uri",
                input=f"{tmpd}/input.dump",
                output=f"{tmpd}/output.dump",
                error=f"{tmpd}/error.dump",
                n_iters=-1,
            )
            with open(conf.input, "w") as f:
                f.write("foo")
            i = 0

            def send_to_remote(uri, data):
                nonlocal i
                i += 1
                if i < 3:
                    return None, "testing0"
                return "qwer", "testing1"

            with patch.object(adapter.Adapter, "send_to_remote", new=send_to_remote):
                status = adapter.Adapter.cli(conf)
                assert status == 0
                with open(conf.output, "r") as f:
                    assert f.read() == "qwer"
                with open(conf.error, "r") as f:
                    assert f.read().strip() == "testing0\ntesting0\ntesting1"

    def test_git_info(self):
        with Dml.temporary() as dml:
            d0 = dml.new("d0", "d0")
            git_info = d0[".dml/git"].value()
            assert isinstance(git_info, dict)
            self.assertCountEqual(git_info.keys(), ["branch", "commit", "remote", "status"])
            assert all(type(x) is str for x in git_info.values())

    def test_funkify(self):
        def fn(*args):
            return sum(args)

        @funkify(extra_fns=[fn])
        def dag_fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = fn(*dag.argv[1:].value())
            return dag.result

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                vals = [1, 2, 3]
                with dml.new("d0", "d0") as d0:
                    d0.f0 = dag_fn
                    d0.n0 = d0.f0(*vals)
                    assert d0.n0.value() == sum(vals)
                    # you can get the original back
                    d0.f1 = funkify(dag_fn.fn, extra_fns=[fn])
                    d0.n1 = d0.f1(*vals)
                    assert d0.n1.value() == sum(vals)
                    dag = dml.load(d0.n1)
                    assert dag.result is not None
                dag = dml("dag", "describe", dag._ref.to)

    def test_funkify_logs(self):
        @funkify
        def dag_fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())
            return dag.result

        client = boto3.client("logs", endpoint_url=self.moto_endpoint)
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                vals = [1, 2, 3]
                with dml.new("d0", "d0") as d0:
                    d0.f0 = dag_fn
                    node = d0.f0(*vals)
                    dag = node.load()
                config = dag[".dml/env"].value()
        logs = client.get_log_events(logGroupName=config["log_group"], logStreamName=config["log_stdout"])["events"]
        self.assertCountEqual(
            [
                f"*** Starting {config['run_id']} ***",
                "testing stdout...",
                f"*** Ending {config['run_id']} ***",
            ],
            {x["message"] for x in logs},
        )
        logs = client.get_log_events(logGroupName=config["log_group"], logStreamName=config["log_stderr"])["events"]
        assert "testing stderr..." in {x["message"] for x in logs}

    def test_funkify_string(self):
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                vals = [1, 2, 3]
                with dml.new("d0", "d0") as dag:
                    dag.f0 = funkify(
                        dedent(
                            """
                        from dml_util import aws_fndag

                        if __name__ == "__main__":
                            with aws_fndag() as dag:
                                dag.n0 = sum(dag.argv[1:].value())
                                dag.result = dag.n0
                            """
                        ).strip(),
                    )
                    dag.n0 = dag.f0(*vals)
                    assert dag.n0.value() == sum(vals)
                    dag.result = dag.n0
                dag = dml.load(dag.n0)
                dag = dml("dag", "describe", dag._ref.to)

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

    def test_funkify_errors(self):
        @funkify
        def dag_fn(dag):
            dag.result = dag.argv[1].value() / dag.argv[-1].value()
            return dag.result

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                d0 = dml.new("d0", "d0")
                d0.f0 = dag_fn
                with self.assertRaisesRegex(Error, "division by zero"):
                    d0.n0 = d0.f0(1, 0)


class TestFunks(FullDmlTestCase):
    @skipIf(not shutil.which("hatch"), "hatch is not available")
    def test_hatch_script_passes_env(self):
        js = HatchRunner.funkify("pandas", None)
        print(js["script"])
        # assert False
        assert isinstance(js, dict)
        resp = subprocess.run(
            ["bash", "-c", js["script"], "script", "env"],
            # shell=True,
            text=True,
            capture_output=True,
            env={"DML_CACHE_KEY": "test_key", "DML_CACHE_PATH": self.tmpd.name},
        )
        lines = resp.stdout.splitlines()
        env = {k: v for k, v in (x.split("=", 1) for x in lines) if k.startswith("DML_")}
        assert env["DML_CACHE_KEY"] == "test_key"
        assert env["DML_CACHE_PATH"] == self.tmpd.name

    @skipIf(not shutil.which("hatch"), "hatch is not available")
    def test_hatch(self):
        @funkify(uri="hatch", data={"name": "pandas", "path": str(_root_)})
        @funkify
        def dag_fn(dag):
            import pandas as pd

            print("testing stdout...")
            return pd.__version__

        logc = boto3.client("logs", endpoint_url=self.moto_endpoint)
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                d0 = dml.new("d0", "d0")
                d0.f0 = dag_fn
                result = d0.f0()
                assert VALID_VERSION.match(result.value())
                streams = json.loads(result.load()[".dml/env"].value()["log_streams"])
                assert all(x.startswith("/run/") for x in streams)
                suffixes = [x.split("/")[-1] for x in streams]
                assert len(streams) == 3, f"Expected 3 log streams, got {len(streams)}: {suffixes}"
        logs = logc.get_log_events(logGroupName="dml", logStreamName=streams[-1])["events"]
        assert len(logs) == 3
        assert logs[1]["message"] == "testing stdout..."

    @skipIf(not shutil.which("conda"), "conda is not available")
    def test_conda(self):
        with self.assertRaisesRegex(ModuleNotFoundError, "No module named 'pandas'"):
            import pandas  # noqa: F401

        @funkify(
            uri="conda",
            data={"name": "dml-pandas"},
        )
        @funkify
        def dag_fn(dag):
            import pandas as pd

            return pd.__version__

        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                d0 = dml.new("d0", "d0")
                d0.f0 = dag_fn
                result = d0.f0()
                assert VALID_VERSION.match(result.value())
                tmp = json.loads(result.load()[".dml/env"].value()["log_streams"])
                assert len(tmp) == 3

    @skipIf(not shutil.which("conda"), "conda is not available")
    @skipIf(not shutil.which("hatch"), "hatch is not available")
    def test_conda_in_hatch(self):
        with self.assertRaisesRegex(ModuleNotFoundError, "No module named 'pandas'"):
            import pandas  # noqa: F401

        @funkify(uri="conda", data={"name": "dml-pandas"})
        @funkify
        def dag_fn(dag):
            import pandas as pd

            return pd.__version__

        @funkify(uri="hatch", data={"name": "default", "path": str(_root_)})
        @funkify
        def dag_fn2(dag):
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
                tmp = json.loads(result.load()[".dml/env"].value()["log_streams"])
                assert len(tmp) == 3

    @skipIf(not shutil.which("conda"), "conda is not available")
    @skipIf(not shutil.which("hatch"), "hatch is not available")
    def test_hatch_in_conda(self):
        with self.assertRaisesRegex(ModuleNotFoundError, "No module named 'polars'"):
            import polars  # noqa: F401

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

    def test_docker_patched(self):
        os.environ["DML_CACHE_KEY"] = "foo:key"
        data = {
            "cache_key": "foo:key",
            "cache_path": "bar",
            "kwargs": {
                "sub": {"uri": "bar", "data": {}, "adapter": "baz"},
                "image": {"uri": "foo:uri"},
            },
            "dump": "opaque",
        }
        with tmpdir() as tmpd:
            conf = Config(
                uri="docker",
                input=f"{tmpd}/input.dump",
                output=f"{tmpd}/output.dump",
                error=f"{tmpd}/error.dump",
                n_iters=1,
            )
            with open(conf.input, "w") as f:
                json.dump(data, f)
            with patch.object(DockerRunner, "start_docker", return_value="testing0"):
                status = adapter.LocalAdapter.cli(conf)
                assert status == 0
                assert not os.path.exists(conf.output)
                with open(conf.error, "r") as f:
                    tmp = f.read().strip()
                    assert "foo:key" in tmp
                    assert "container testing0 started" in tmp
                os.truncate(conf.error, 0)
            sc = DockerRunner.state_class(data["cache_key"])
            assert sc.get()["cid"] == "testing0"
            docker_tmpd = Path(sc.get()["tmpd"])
            with patch.object(DockerRunner, "get_docker_status", return_value="running"):
                status = adapter.LocalAdapter.cli(conf)
                assert status == 0
                assert not os.path.exists(conf.output)
                with open(conf.error, "r") as f:
                    tmp = f.read().strip()
                assert "foo:key" in tmp
                assert "container testing0 running" in tmp
                os.truncate(conf.error, 0)
            with patch.object(DockerRunner, "get_docker_status", return_value="exited"):
                with open(docker_tmpd / DockerRunner._file_names[1], "w") as f:
                    json.dump({"dump": "qwer"}, f)
                status = adapter.LocalAdapter.cli(conf)
                assert status == 0
                with open(conf.error, "r") as f:
                    tmp = f.read().strip()
                assert "foo:key" in tmp
                assert "container testing0 finished with status 'exited'" in tmp
                with open(conf.output, "r") as f:
                    assert json.load(f)["dump"] == "qwer"
            sc.delete()
            os.remove(conf.output)
            os.remove(conf.error)
            with patch.object(DockerRunner, "start_docker", return_value="testing0"):
                status = adapter.LocalAdapter.cli(conf)
                assert status == 0
                assert not os.path.exists(conf.output)
                with open(conf.error, "r") as f:
                    assert f.read().strip() == "dockerrunner [foo:key] :: container testing0 started"
                os.truncate(conf.error, 0)
            sc = DockerRunner.state_class(data["cache_key"])
            docker_tmpd = Path(sc.get()["tmpd"])
            with patch.object(DockerRunner, "get_docker_status", return_value="exited"):
                with patch.object(DockerRunner, "get_docker_exit_code", return_value=1):
                    with open(docker_tmpd / DockerRunner._file_names[2], "w") as f:
                        f.write("testing stderr...")
                    status = adapter.LocalAdapter.cli(conf)
                    assert status == 1
                    with open(conf.error, "r") as f:
                        assert "testing stderr" in f.read()

    def test_docker_patched2(self):
        with tmpdir() as tmpd:

            @funkify(uri="test", data={"image": Resource(tmpd)})
            @funkify
            def fn(dag):
                return sum(dag.argv[1:].value())

            vals = [1, 2, 3]
            with TemporaryDirectory(prefix="dml-util-test-") as tmpc:
                with Dml.temporary(cache_path=tmpc) as dml:
                    dag = dml.new("test", "asdf")
                    dag.fn = fn
                    dag.n0 = dag.fn(*vals)
                    assert dag.n0.value() == sum(vals)
                    dag2 = dml.load(dag.n0)
                    assert dag2.result is not None
                    dag2 = dml("dag", "describe", dag2._ref.to)

    @skipIf(not shutil.which("docker"), "docker not available")
    def test_docker_build(self):
        @funkify
        def fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())

        with open(f"{self.tmpd.name}/credentials", "w") as f:
            f.write("[default]\n")
            f.write(f"aws_access_key_id={self.aws_env['AWS_ACCESS_KEY_ID']}\n")
            f.write(f"aws_secret_access_key={self.aws_env['AWS_SECRET_ACCESS_KEY']}\n")
        with open(f"{self.tmpd.name}/config", "w") as f:
            f.write("[default]\n")
            f.write(f"region={self.aws_env['AWS_DEFAULT_REGION']}\n")
        flags = [
            "--platform",
            "linux/amd64",
            "--add-host=host.docker.internal:host-gateway",
            "-e",
            f"AWS_ENDPOINT_URL=http://host.docker.internal:{self.moto_port}",
            "-v",
            f"{shlex.quote(self.tmpd.name)}/credentials:/root/.aws/credentials:ro",
            "-v",
            f"{shlex.quote(self.tmpd.name)}/config:/root/.aws/config:ro",
        ]
        s3 = S3Store()
        vals = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                dag = dml.new("test", "asdf")
                excludes = ["tests/*.py", ".pytest_cache", ".ruff_cache", "__pycache__"]
                dag.tar = s3.tar(dml, _root_, excludes=excludes)
                dag.img = Ecr().build(
                    dag.tar.value(),
                    [
                        "--platform",
                        "linux/amd64",
                        "-f",
                        "tests/assets/dkr-context/Dockerfile",
                    ],
                )["image"]
                assert isinstance(dag.img.value(), Resource)
                dag.fn = funkify(
                    fn,
                    "docker",
                    {"image": dag.img.value(), "flags": flags},
                    adapter="local",
                )
                dag.baz = dag.fn(*vals)
                assert dag.baz.value() == sum(vals)
                dag2 = dml.load(dag.baz)
                assert dag2.result is not None
                dag2 = dml("dag", "describe", dag2._ref.to)

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
                self.assertCountEqual(dag.stack.keys().value(), ["BucketName", "BucketArn"])
                dag.result = dag.stack


class TestSSH(FullDmlTestCase):
    def setUp(self):
        super().setUp()
        self.tmpdir = mkdtemp()
        self.fn_cache_dir = mkdtemp()

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
        sock.close()

        host_key_path = os.path.join(self.tmpdir, "ssh_host_rsa_key")
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-f", host_key_path],
            check=True,
        )

        client_key_path = os.path.join(self.tmpdir, "client_key")
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-f", client_key_path],
            check=True,
        )

        authorized_keys_path = os.path.join(self.tmpdir, "authorized_keys")
        client_pub_key_path = client_key_path + ".pub"
        shutil.copy(client_pub_key_path, authorized_keys_path)
        os.chmod(authorized_keys_path, 0o600)
        sshd_config_path = os.path.join(self.tmpdir, "sshd_config")
        pid_file = os.path.join(self.tmpdir, "sshd.pid")
        with open(sshd_config_path, "w") as f:
            f.write(
                dedent(
                    f"""
                    Port {port}
                    ListenAddress 127.0.0.1
                    HostKey {host_key_path}
                    PidFile {pid_file}
                    LogLevel DEBUG
                    UsePrivilegeSeparation no
                    StrictModes no
                    PasswordAuthentication no
                    ChallengeResponseAuthentication no
                    PubkeyAuthentication yes
                    AuthorizedKeysFile {authorized_keys_path}
                    UsePAM no
                    Subsystem sftp internal-sftp
                    """
                ).strip()
            )

        self.sshd_proc = subprocess.Popen(
            [shutil.which("sshd"), "-f", sshd_config_path, "-D"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.flags = [
            "-i",
            client_key_path,
            "-p",
            str(port),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        ]
        self.env_file = os.path.join(self.tmpdir, "env_file")
        with open(self.env_file, "w") as f:
            f.write(
                dedent(
                    f"""
                    export DML_FN_CACHE_DIR={self.fn_cache_dir}
                    export PATH={shlex.quote(str(Path(sys.executable).parent))}:$PATH
                    export PATH={shlex.quote(os.path.dirname(shutil.which("docker")))}:$PATH
                    """
                ).strip()
            )
            for k, v in self.aws_env.items():
                if not k.startswith("AWS_"):
                    continue
                f.write(f"\nexport {k}={v}")
        self.resource_data = {
            "host": f"{getpass.getuser()}@127.0.0.1",
            "flags": self.flags,
            "env_files": [self.env_file],
        }

        deadline = time.time() + 5  # wait up to 5 seconds
        while time.time() < deadline:
            if self.sshd_proc.poll() is not None:
                stdout, stderr = self.sshd_proc.communicate(timeout=1)
                raise RuntimeError(
                    f"sshd terminated unexpectedly.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
                )
            try:
                test_sock = socket.create_connection(("127.0.0.1", port), timeout=0.5)
                test_sock.close()
                break
            except (ConnectionRefusedError, OSError):
                time.sleep(0.1)
        else:
            raise RuntimeError("Timeout waiting for sshd to start.")

    def tearDown(self):
        if self.sshd_proc:
            self.sshd_proc.terminate()
            try:
                self.sshd_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.sshd_proc.kill()
        shutil.rmtree(self.tmpdir)
        shutil.rmtree(self.fn_cache_dir)
        super().tearDown()

    def test_ssh(self):
        @funkify(uri="ssh", data=self.resource_data)
        @funkify(uri="hatch", data={"name": "pandas", "path": str(_root_)})
        @funkify
        def fn(dag):
            import sys

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

    @skipIf(not shutil.which("docker"), "docker not available")
    def test_docker_build(self):
        @funkify
        def fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())

        with open(f"{self.tmpd.name}/credentials", "w") as f:
            f.write("[default]\n")
            f.write(f"aws_access_key_id={self.aws_env['AWS_ACCESS_KEY_ID']}\n")
            f.write(f"aws_secret_access_key={self.aws_env['AWS_SECRET_ACCESS_KEY']}\n")
        with open(f"{self.tmpd.name}/config", "w") as f:
            f.write("[default]\n")
            f.write(f"region={self.aws_env['AWS_DEFAULT_REGION']}\n")
        flags = [
            "--platform",
            "linux/amd64",
            "--add-host=host.docker.internal:host-gateway",
            "-e",
            f"AWS_ENDPOINT_URL=http://host.docker.internal:{self.moto_port}",
            "-v",
            f"{shlex.quote(self.tmpd.name)}/credentials:/root/.aws/credentials:ro",
            "-v",
            f"{shlex.quote(self.tmpd.name)}/config:/root/.aws/config:ro",
        ]
        dkr_build_in_hatch = funkify(funk.dkr_build, "hatch", data={"name": "default", "path": str(_root_)})
        s3 = S3Store()
        vals = [1, 2, 3]
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            with Dml.temporary(cache_path=tmpd) as dml:
                dag = dml.new("test", "asdf")
                dag.tar = s3.tar(dml, _root_, excludes=["tests/*.py"])
                dag.dkr = funkify(dkr_build_in_hatch, uri="ssh", data=self.resource_data)
                dag.img = dag.dkr(
                    dag.tar,
                    [
                        "--platform",
                        "linux/amd64",
                        "-f",
                        "tests/assets/dkr-context/Dockerfile",
                    ],
                )
                dag.fn = funkify(
                    funkify(
                        funkify(
                            fn,
                            "docker",
                            {"image": dag.img.value(), "flags": flags},
                            adapter="local",
                        ),
                        uri="hatch",
                        data={"name": "default", "path": str(_root_)},
                    ),
                    uri="ssh",
                    data=self.resource_data,
                )
                dag.baz = dag.fn(*vals)
                assert dag.baz.value() == sum(vals)
                dag2 = dml.load(dag.baz)
                assert dag2.result is not None
                dag2 = dml("dag", "describe", dag2._ref.to)
