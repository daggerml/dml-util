import os
import subprocess
from glob import glob
from pathlib import Path
from shutil import which
from tempfile import NamedTemporaryFile, TemporaryDirectory
from unittest import TestCase, mock, skipUnless
from urllib.parse import urlparse

import boto3
from daggerml.core import Dml, Error

from dml_util import DOCKER_EXEC, S3, dkr_build, funkify, query_update
from dml_util.common import BUCKET, PREFIX

_root_ = Path(__file__).parent.parent

try:
    import docker  # noqa: F401

    has_docker = True
except ImportError:
    has_docker = which("docker")


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f"{path}/**", recursive=True)]


def untar(s3_tar, dest):
    p = urlparse(s3_tar.uri)
    with NamedTemporaryFile(suffix=".tar") as tmpf:
        boto3.client("s3").download_file(p.netloc, p.path[1:], tmpf.name)
        subprocess.run(["tar", "-xvf", tmpf.name, "-C", dest], check=True)


class TestAws(TestCase):
    def setUp(self):
        # clear out env variables for safety
        for k in sorted(os.environ.keys()):
            if k.startswith("AWS_"):
                del os.environ[k]
        # os.environ["AWS_PROFILE"] = "daddy"
        # os.environ["AWS_ENDPOINT_URL"] = os.environ["LOCALSTACK_ENDPOINT"] = "http://localhost:4566"
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "bar"
        os.environ["AWS_REGION"] = os.environ["AWS_DEFAULT_REGION"] = "us-west-2"
        # this loads env vars, so import after clearing
        from moto.server import ThreadedMotoServer

        super().setUp()
        self.server = ThreadedMotoServer(port=0)
        self.server.start()
        self.moto_host, self.moto_port = self.server._server.server_address
        self.endpoint = f"http://{self.moto_host}:{self.moto_port}"
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=BUCKET)

    def test_tar(self):
        context = _root_ / "tests/assets/dkr-context"
        s3 = S3()
        assert s3.bucket == BUCKET
        assert s3.prefix == PREFIX
        with Dml() as dml:
            s3_tar = s3.tar(dml, context)
            with TemporaryDirectory() as tmpd:
                untar(s3_tar, tmpd)
                assert ls_r(tmpd) == ls_r(context)
            # consistent hash
            s3_tar2 = s3.tar(dml, context)
            assert s3_tar.uri == s3_tar2.uri

    @skipUnless(has_docker, "docker not available")
    def test_docker_build(self):
        context = _root_ / "tests/assets/dkr-context"

        def fn(dag):
            dag.result = sum(dag.argv[1:].value())

        s3 = S3()
        vals = [1, 2, 3]
        with Dml() as dml:
            with dml.new("test", "asdf") as dag:
                dag.tar = s3.tar(dml, context)
                dag.dkr = dkr_build
                dag.img = dag.dkr(
                    dag.tar,
                    ["--platform", "linux/amd64"],
                )
                dag.chg = query_update
                dag.foo = dag.chg(DOCKER_EXEC, {"image": dag.img})
                dag.bar = funkify(fn, dag.foo.value(), params={"flags": ["--platform", "linux/amd64"]})
                dag.baz = dag.bar(*vals)
                assert dag.baz.value() == sum(vals)

        # repo_uri = boto3.client("ecr").create_repository(repositoryName="test")["repository"]["repositoryUri"]
        # dag.psh = dkr_push
        # dag.qux = dag.psh(dag.img, repo_uri)

    def tearDown(self):
        self.server.stop()
        super().tearDown()


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
            dag.result = sum(*dag.argv[1:].value()) / 0
            return dag.result

        with TemporaryDirectory() as fn_cache_dir:
            with mock.patch.dict(os.environ, DML_FN_CACHE_DIR=fn_cache_dir):
                with Dml() as dml:
                    d0 = dml.new("d0", "d0")
                    d0.f0 = dag_fn
                    with self.assertRaises(Error):
                        d0.n0 = d0.f0(1, 2, 3)
