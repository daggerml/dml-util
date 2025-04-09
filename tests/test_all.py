import getpass
import os
import shutil
import socket
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent
from unittest import skipIf

import boto3
from daggerml import Dml, Resource
from daggerml.core import Error

from dml_util import S3Store, funk, funkify
from dml_util.baseutil import S3_BUCKET, S3_PREFIX
from tests.test_baseutil import AwsTestCase

try:
    import docker  # noqa: F401
except ImportError:
    docker = None

_root_ = Path(__file__).parent.parent

# TODO: write unit tests for everything
# TODO: Write a test-adapter that will write to a file and communicate via log messages


class FullDmlTestCase(AwsTestCase):
    def setUp(self):
        super().setUp()
        boto3.client("s3", endpoint_url=self.moto_endpoint).create_bucket(Bucket=S3_BUCKET)
        for key in ["DML_REPO", "DML_CONFIG_DIR", "DML_PROJECT_DIR"]:
            if key in os.environ:
                del os.environ[key]
        self.tmpd = TemporaryDirectory()
        os.environ["DML_FN_CACHE_DIR"] = self.tmpd.name

    def tearDown(self):
        s3 = S3Store()
        s3.rm(*s3.ls(recursive=True))
        self.tmpd.cleanup()
        super().tearDown()


class TestFunks(FullDmlTestCase):
    def test_s3_uri(self):
        s3 = S3Store()
        raw = b"foo bar baz"
        resp = s3.put(raw, name="foo.txt")
        assert resp.uri == f"s3://{S3_BUCKET}/{S3_PREFIX}/data/foo.txt"
        resp = s3.put(raw, uri=f"s3://{S3_BUCKET}/asdf/foo.txt")
        assert resp.uri == f"s3://{S3_BUCKET}/asdf/foo.txt"

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

        s3 = S3Store()
        with Dml() as dml:
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
            dag = dml("dag", "describe", dag._ref.to.split("/")[-1])
            logs = {k: s3.get(v).decode().strip() for k, v in dag["logs"].items()}
            assert logs == {x: f"testing {x}..." for x in ["stdout", "stderr"]}

    def test_executor_caching_success(self):
        @funkify
        def dag_fn(dag):
            from uuid import uuid4

            return uuid4().hex

        vals = [1, 2, 3]
        with Dml() as dml:
            with dml.new("d0", "d0") as d0:
                d0.f0 = dag_fn
                d0.n0 = d0.f0(*vals)
                hex0 = d0.n0.value()
        # new database
        with Dml() as dml:
            with dml.new("d0", "d0") as d0:
                d0.f0 = dag_fn
                d0.n0 = d0.f0(*vals)
                hex1 = d0.n0.value()
        assert hex0 == hex1

    def test_executor_caching_my_error(self):
        @funkify
        def dag_fn(dag):
            from uuid import uuid4

            raise RuntimeError(f"dml: {uuid4().hex}")

        vals = [1, 2, 3]
        with Dml() as dml:
            d0 = dml.new("d0", "d0")
            d0.f0 = dag_fn
            with self.assertRaisesRegex(Error, "dml:") as e:
                d0.n0 = d0.f0(*vals)
            err_msg0 = str(e.exception.message)
        # new database
        with Dml() as dml:
            d0 = dml.new("d0", "d0")
            d0.f0 = dag_fn
            with self.assertRaisesRegex(Error, "dml:") as e:
                d0.n0 = d0.f0(*vals)
            err_msg1 = str(e.exception.message)
        assert err_msg0 == err_msg1

    def test_executor_not_caching_adapter_error(self):
        @funkify
        def dag_fn(dag):
            from uuid import uuid4

            raise RuntimeError(f"dml: {uuid4().hex}")

        # corrupting the script so the adapter fails
        dag_fn.data["script"] = dag_fn.data["script"][50:]

        vals = [1, 2, 3]
        with Dml() as dml:
            d0 = dml.new("d0", "d0")
            d0.f0 = dag_fn
            with self.assertRaisesRegex(Error, "exit status") as e:
                d0.n0 = d0.f0(*vals)
            err_msg0 = str(e.exception.message)
        # new database
        with Dml() as dml:
            d0 = dml.new("d0", "d0")
            d0.f0 = dag_fn
            with self.assertRaisesRegex(Error, "exit status") as e:
                d0.n0 = d0.f0(*vals)
            err_msg1 = str(e.exception.message)
        assert err_msg0 != err_msg1

    def test_funkify_string(self):
        s3 = S3Store()
        with Dml() as dml:
            vals = [1, 2, 3]
            with dml.new("d0", "d0") as dag:
                dag.f0 = funkify(
                    dedent(
                        """
                    import sys
                    print("testing stdout...")
                    print("testing stderr...", file=sys.stderr)

                    from dml_util.adapter import aws_fndag

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
            dag = dml("dag", "describe", dag._ref.to.split("/")[-1])
            logs = {k: s3.get(v).decode().strip() for k, v in dag["logs"].items()}
            assert logs == {x: f"testing {x}..." for x in ["stdout", "stderr"]}

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
        with Dml() as dml:
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

        with Dml() as dml:
            d0 = dml.new("d0", "d0")
            d0.f0 = dag_fn
            with self.assertRaisesRegex(Error, "division by zero"):
                d0.n0 = d0.f0(1, 0)

    @skipIf(not shutil.which("hatch"), "hatch is not available")
    @skipIf(os.getenv("GITHUB_ACTIONS"), "github actions + docker interaction")
    def test_funkify_hatch(self):
        @funkify(
            uri="hatch",
            data={
                "name": "pandas",
                "path": str(_root_),
                "env": {
                    "DML_FN_CACHE_DIR": self.tmpd.name,
                    "AWS_ENDPOINT_URL": self.moto_endpoint,
                },
            },
        )
        @funkify
        def dag_fn(dag):
            import pandas as pd

            dag.result = pd.__version__
            return dag.result

        with Dml() as dml:
            d0 = dml.new("d0", "d0")
            d0.f0 = dag_fn
            d0.result = d0.f0()
            assert d0.result.value() == "2.2.3"

    @skipIf(not shutil.which("conda"), "conda is not available")
    @skipIf(os.getenv("GITHUB_ACTIONS"), "github actions + docker interaction")
    def test_funkify_conda(self):
        @funkify
        def dag_fn(dag):
            import pandas as pd

            return pd.Series({f"x{i}": i for i in dag.argv[1:].value()}).to_dict()

        vals = [1, 2, 3]
        with Dml() as dml:
            d0 = dml.new("d0", "d0")
            d0.f0 = dag_fn
            with self.assertRaisesRegex(Error, "No module named 'pandas'"):
                d0.f0()
            d0.f1 = funkify(
                dag_fn,
                "conda",
                data={
                    "name": "dml-pandas",
                    "env": {
                        "DML_FN_CACHE_DIR": self.tmpd.name,
                        "AWS_ENDPOINT_URL": self.moto_endpoint,
                    },
                },
            )
            d0.result = d0.f1(*vals)
            assert d0.result.value() == {f"x{i}": i for i in vals}

    @skipIf(docker is None, "docker not available")
    @skipIf(os.getenv("GITHUB_ACTIONS"), "github actions + docker interaction")
    def test_docker_build(self):
        from dml_util import dkr_build, funkify

        def fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())

        flags = [
            "--platform",
            "linux/amd64",
            "-e",
            f"AWS_ENDPOINT_URL=http://host.docker.internal:{self.moto_port}",
            "-p",
            f"{self.moto_port}:{self.moto_port}",
        ]
        host_ip = subprocess.run(
            "ip route | awk '/default/ {print $3}'",
            shell=True,
            capture_output=True,
            check=True,
            text=True,
        ).stdout.strip()
        if host_ip:
            print(f"{host_ip = !r}")
            flags.append(f"--add-host=host.docker.internal:{host_ip}")

        s3 = S3Store()
        vals = [1, 2, 3]
        with Dml() as dml:
            with dml.new("test", "asdf") as dag:
                dag.tar = s3.tar(dml, _root_, excludes=["tests/*.py"])
                dag.dkr = dkr_build
                dag.img = dag.dkr(
                    dag.tar,
                    [
                        "--platform",
                        "linux/amd64",
                        "-f",
                        "tests/assets/dkr-context/Dockerfile",
                    ],
                )
                dag.fn0 = funkify(fn)
                fn0 = dag.fn0.value()
                # fn0 = Resource("test", fn0.data, fn0.adapter)
                dag.fn = funkify(
                    fn0,
                    "docker",
                    {"image": dag.img.value(), "flags": flags},
                    adapter="local",
                )
                dag.baz = dag.fn(*vals)
                assert dag.baz.value() == sum(vals)
                dag2 = dml.load(dag.baz)
                assert dag2.result is not None
            dag2 = dml("dag", "describe", dag2._ref.to.split("/")[-1])
            logs = {k: s3.get(v).decode().strip() for k, v in dag2["logs"].items()}
            self.assertCountEqual(logs.keys(), ["stdout", "stderr", "docker/combined"])
            assert logs["stdout"] == "testing stdout..."
            assert logs["stderr"] == "testing stderr..."

    def test_notebooks(self):
        s3 = S3Store()
        with Dml() as dml:
            dag = dml.new("bar")
            dag.nb = s3.put(filepath=_root_ / "tests/assets/notebook.ipynb", suffix=".ipynb")
            dag.nb_exec = funk.execute_notebook
            dag.html = dag.nb_exec(dag.nb)
            dag.result = dag.html

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
        with Dml() as dml:
            dag = dml.new("foo")
            dag.cfn = Resource("cfn", adapter="dml-util-local-adapter")
            dag.stack = dag.cfn("stacker", tpl, {})
            self.assertCountEqual(dag.stack.keys().value(), ["BucketName", "BucketArn"])
            dag.result = dag.stack


# @skipIf(True, "ssh needs some work")
class TestSSH(FullDmlTestCase):
    def setUp(self):
        super().setUp()
        # Create a temporary directory for our files.
        self.tmpdir = tempfile.mkdtemp()

        # Determine a free port on localhost.
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        sock.close()

        # Generate the sshd host key.
        self.host_key_path = os.path.join(self.tmpdir, "ssh_host_rsa_key")
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-f", self.host_key_path],
            check=True,
        )

        # Generate a client key pair.
        self.client_key_path = os.path.join(self.tmpdir, "client_key")
        subprocess.run(
            ["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-f", self.client_key_path],
            check=True,
        )

        # Create an authorized_keys file using the client's public key.
        self.authorized_keys_path = os.path.join(self.tmpdir, "authorized_keys")
        client_pub_key_path = self.client_key_path + ".pub"
        shutil.copy(client_pub_key_path, self.authorized_keys_path)
        os.chmod(self.authorized_keys_path, 0o600)

        # Get the current username (make sure this user exists on the system).
        self.user = getpass.getuser()

        # Write a minimal sshd configuration file.
        self.sshd_config_path = os.path.join(self.tmpdir, "sshd_config")
        pid_file = os.path.join(self.tmpdir, "sshd.pid")
        with open(self.sshd_config_path, "w") as f:
            f.write(
                dedent(
                    f"""
                    Port {self.port}
                    ListenAddress 127.0.0.1
                    HostKey {self.host_key_path}
                    PidFile {pid_file}
                    LogLevel DEBUG
                    UsePrivilegeSeparation no
                    StrictModes no
                    PasswordAuthentication no
                    ChallengeResponseAuthentication no
                    PubkeyAuthentication yes
                    AuthorizedKeysFile {self.authorized_keys_path}
                    UsePAM no
                    Subsystem sftp internal-sftp
                    """
                ).strip()
            )

        # Start sshd using the temporary configuration.
        self.sshd_proc = subprocess.Popen(
            [shutil.which("sshd"), "-f", self.sshd_config_path, "-D"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        self.flags = [
            "-i",
            self.client_key_path,
            "-p",
            str(self.port),
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            "UserKnownHostsFile=/dev/null",
        ]
        self.resource_data = {
            "user": self.user,
            "host": "127.0.0.1",
            "flags": self.flags,
        }

        # Wait until the server is ready by polling the port.
        deadline = time.time() + 5  # wait up to 5 seconds
        while time.time() < deadline:
            # If sshd died, capture its output for debugging.
            if self.sshd_proc.poll() is not None:
                stdout, stderr = self.sshd_proc.communicate(timeout=1)
                raise RuntimeError(
                    f"sshd terminated unexpectedly.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
                )
            try:
                test_sock = socket.create_connection(("127.0.0.1", self.port), timeout=0.5)
                test_sock.close()
                break
            except (ConnectionRefusedError, OSError):
                time.sleep(0.5)
        else:
            raise RuntimeError("Timeout waiting for sshd to start.")
        self.uri = f"{self.user}@127.0.0.1"

    def tearDown(self):
        # Terminate the sshd process.
        if self.sshd_proc:
            self.sshd_proc.terminate()
            try:
                self.sshd_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.sshd_proc.kill()
        # Clean up temporary files.
        shutil.rmtree(self.tmpdir)
        super().tearDown()

    @skipIf(os.getenv("GITHUB_ACTIONS"), "github actions is messed up")
    def test_ssh(self):
        @funkify(
            uri="ssh",
            data=self.resource_data,
        )
        @funkify(
            uri="hatch",
            data={
                "name": "pandas",
                "path": str(_root_),
                "env": {
                    "DML_FN_CACHE_DIR": self.tmpd.name,
                    "AWS_ENDPOINT_URL": self.moto_endpoint,
                },
            },
        )
        @funkify
        def fn(dag):
            import pandas as pd

            return pd.Series({f"x{i}": i for i in dag.argv[1:].value()}).to_dict()

        vals = [1, 2, 3]
        with Dml() as dml:
            with dml.new("test", "asdf") as dag:
                dag.fn = fn
                dag.ans = dag.fn(*vals)
                assert dag.ans.value() == {f"x{i}": i for i in vals}

    @skipIf(docker is None, "docker not available")
    @skipIf(os.getenv("GITHUB_ACTIONS"), "github actions + docker interaction")
    def test_docker_build(self):
        from dml_util import dkr_build, funkify

        def fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = sum(dag.argv[1:].value())

        flags = [
            "--platform",
            "linux/amd64",
            "-e",
            f"AWS_ENDPOINT_URL=http://host.docker.internal:{self.moto_port}",
            "-p",
            f"{self.moto_port}:{self.moto_port}",
        ]
        host_ip = subprocess.run(
            "ip route | awk '/default/ {print $3}'",
            shell=True,
            capture_output=True,
            check=True,
            text=True,
        ).stdout.strip()
        if host_ip:
            print(f"{host_ip = !r}")
            flags.append(f"--add-host=host.docker.internal:{host_ip}")

        dkr_build_in_hatch = funkify(
            dkr_build,
            "hatch",
            data={
                "name": "pandas",
                "path": str(_root_),
                "env": {
                    "DML_FN_CACHE_DIR": self.tmpd.name,
                    **self.aws_env,
                },
            },
        )
        s3 = S3Store()
        vals = [1, 2, 3]
        with Dml() as dml:
            with dml.new("test", "asdf") as dag:
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
                dag.fn0 = funkify(fn)
                fn0 = dag.fn0.value()
                # fn0 = Resource("test", fn0.data, fn0.adapter)
                dag.fn = funkify(
                    fn0,
                    "docker",
                    {"image": dag.img.value(), "flags": flags},
                    adapter="local",
                )
                dag.baz = dag.fn(*vals)
                assert dag.baz.value() == sum(vals)
                dag2 = dml.load(dag.baz)
                assert dag2.result is not None
            dag2 = dml("dag", "describe", dag2._ref.to.split("/")[-1])
            logs = {k: s3.get(v).decode().strip() for k, v in dag2["logs"].items()}
            self.assertCountEqual(logs.keys(), ["stdout", "stderr", "docker/combined"])
            assert logs["stdout"] == "testing stdout..."
            assert logs["stderr"] == "testing stderr..."
