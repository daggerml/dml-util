import os
from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from unittest import TestCase, skipIf

import boto3

import dml_util.baseutil as baseutil
from dml_util import S3Store
from dml_util.baseutil import S3_BUCKET, S3_PREFIX

_root_ = Path(__file__).parent.parent

try:
    import docker  # noqa: F401
except ImportError:
    docker = None

try:
    from daggerml.core import Dml
except ImportError:
    Dml = None


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f"{path}/**", recursive=True)]


class AwsTestCase(TestCase):
    def setUp(self):
        # clear out env variables for safety
        for k in sorted(os.environ.keys()):
            if k.startswith("AWS_"):
                del os.environ[k]
        os.environ["AWS_ACCESS_KEY_ID"] = "foo"
        os.environ["AWS_SECRET_ACCESS_KEY"] = "foo"
        self.region = "us-east-1"
        os.environ["AWS_REGION"] = self.region
        os.environ["AWS_DEFAULT_REGION"] = self.region
        # this loads env vars, so import after clearing
        from moto.server import ThreadedMotoServer

        super().setUp()
        self.server = ThreadedMotoServer(port=0)
        self.server.start()
        self.moto_host, self.moto_port = self.server._server.server_address
        self.endpoint = f"http://{self.moto_host}:{self.moto_port}"
        os.environ["AWS_ENDPOINT_URL"] = self.endpoint

    def tearDown(self):
        self.server.stop()
        super().tearDown()


class TestS3(AwsTestCase):
    def setUp(self):
        super().setUp()
        self.client = boto3.client("s3")
        self.client.create_bucket(Bucket=S3_BUCKET)

    def test_js(self):
        s3 = S3Store()
        js = {"asdf": "wef", "as": [32, True]}
        resp = s3.put_js(js)
        if not isinstance(resp, str):
            resp = resp.uri  # Resource = str if no dml
        js2 = s3.get_js(resp)
        assert js == js2

    def test_writeable(self):
        s3 = S3Store()

        txt = "testing"

        def foo(tmpf):
            with open(tmpf, "w") as f:
                f.write(txt)

        resp = s3.writeable(foo, suffix=".foo")
        if not isinstance(resp, str):
            resp = resp.uri  # Resource = str if no dml
        r2 = s3.get(resp).decode()
        assert r2 == txt

    @skipIf(Dml is None, "Dml not available")
    def test_tar(self):
        context = _root_ / "tests/assets/dkr-context"
        s3 = S3Store()
        assert s3.bucket == S3_BUCKET
        assert s3.prefix == S3_PREFIX
        with Dml() as dml:
            s3_tar = s3.tar(dml, context)
            with TemporaryDirectory() as tmpd:
                s3.untar(s3_tar, tmpd)
                assert ls_r(tmpd) == ls_r(context)
            # consistent hash
            s3_tar2 = s3.tar(dml, context)
            assert s3_tar.uri == s3_tar2.uri

    @skipIf(docker is None, "docker not available")
    @skipIf(Dml is None, "Dml not available")
    def test_docker_build(self):
        from dml_util import DOCKER_EXEC, dkr_build, funkify, query_update

        context = _root_ / "tests/assets/dkr-context"

        def fn(dag):
            dag.result = sum(dag.argv[1:].value())

        s3 = S3Store()
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

    def tearDown(self):
        super().tearDown()


class TestDynamo(AwsTestCase):
    def setUp(self):
        super().setUp()
        self.client = boto3.client("dynamodb")
        self.tablename = "test-job"
        resp = self.client.create_table(
            TableName=self.tablename,
            AttributeDefinitions=[{"AttributeName": "cache_key", "AttributeType": "S"}],
            KeySchema=[{"AttributeName": "cache_key", "KeyType": "HASH"}],
            BillingMode="PAY_PER_REQUEST",
        )
        self.tb = resp["TableDescription"]["TableArn"]

    def test_dynamo_db_ops(self):
        data = {"q": "b"}
        db = baseutil.Dynamo("test-key", tb=self.tb)
        info = db.get()
        assert info == {}
        assert db.put(data)
        assert db.get() == data
        assert db.unlock()
        db2 = baseutil.Dynamo("test-key", tb=self.tb)
        assert db2.get() == data

    def test_dynamo_locking(self):
        timeout = 0.05
        db0 = baseutil.Dynamo("test-key", timeout=timeout, tb=self.tb)
        db1 = baseutil.Dynamo("test-key", timeout=timeout, tb=self.tb)
        assert db0.get() == {}
        assert db1.get() is None
        assert db1.put({"asdf": 23}) is False
        assert db0.put({"q": "b"}) is True
        # relocking works
        sleep(timeout)
        assert db1.get() == {"q": "b"}
        assert db0.unlock() is False
        assert db1.unlock() is True

    def tearDown(self):
        self.client.delete_table(TableName=self.tb)
        super().tearDown()
