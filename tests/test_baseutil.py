import os
from functools import reduce
from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory
from time import sleep
from unittest import TestCase, skipIf

import boto3

from dml_util import S3Store
from dml_util.baseutil import S3_BUCKET, S3_PREFIX, DynamoState, dict_product

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
        self.region = "us-east-1"
        # this loads env vars, so import after clearing
        from moto.server import ThreadedMotoServer

        super().setUp()
        self.server = ThreadedMotoServer(port=0)
        self.server.start()
        self.moto_host, self.moto_port = self.server._server.server_address
        self.moto_endpoint = f"http://{self.moto_host}:{self.moto_port}"
        aws_env = {
            "AWS_ACCESS_KEY_ID": "foo",
            "AWS_SECRET_ACCESS_KEY": "foo",
            "AWS_REGION": self.region,
            "AWS_DEFAULT_REGION": self.region,
            "AWS_ENDPOINT_URL": self.moto_endpoint,
        }
        for k, v in aws_env.items():
            os.environ[k] = v
        self.aws_env = aws_env

    def tearDown(self):
        self.server.stop()
        super().tearDown()


class TestS3(AwsTestCase):
    def setUp(self):
        super().setUp()
        boto3.client("s3", endpoint_url=self.moto_endpoint).create_bucket(Bucket=S3_BUCKET)

    def test_js(self):
        s3 = S3Store()
        js = {"asdf": "wef", "as": [32, True]}
        resp = s3.put_js(js)
        if not isinstance(resp, str):
            resp = resp.uri  # Resource = str if no dml
        js2 = s3.get_js(resp)
        assert js == js2

    def test_ls(self):
        s3 = S3Store()
        assert s3.ls(recursive=True) == []
        keys = ["a", "b/c", "b/d", "b/d/e", "f"]
        for key in keys:
            s3.put(b"a", name=key)
        ls = s3.ls(recursive=False, lazy=True)
        assert not isinstance(ls, list)
        assert list(ls) == [s3.name2uri(x) for x in keys if "/" not in x]
        ls = s3.ls(recursive=True)
        assert ls == [s3.name2uri(x) for x in keys]
        [s3.rm(k) for k in keys]
        assert s3.ls(recursive=True) == []

    @skipIf(Dml is None, "Dml not available")
    def test_tar(self):
        context = _root_ / "tests/assets/dkr-context"
        s3 = S3Store()
        assert s3.bucket == S3_BUCKET
        assert s3.prefix.startswith(f"{S3_PREFIX}/")
        with Dml() as dml:
            s3_tar = s3.tar(dml, context)
            with TemporaryDirectory() as tmpd:
                s3.untar(s3_tar, tmpd)
                assert ls_r(tmpd) == ls_r(context)
            # consistent hash
            s3_tar2 = s3.tar(dml, context)
            assert s3_tar.uri == s3_tar2.uri

    def tearDown(self):
        s3 = S3Store()
        s3.rm(*s3.ls(recursive=True))
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
        db = DynamoState("test-key", tb=self.tb)
        info = db.get()
        assert info == {}
        assert db.put(data)
        assert db.get() == data
        assert db.unlock()
        db2 = DynamoState("test-key", tb=self.tb)
        assert db2.get() == data

    def test_dynamo_locking(self):
        timeout = 0.05
        db0 = DynamoState("test-key", timeout=timeout, tb=self.tb)
        db1 = DynamoState("test-key", timeout=timeout, tb=self.tb)
        assert db0.get() == {}
        assert db1.get() is None
        assert db1.put({"asdf": 23}) is False
        assert db0.put({"q": "b"}) is True
        # relocking works
        sleep(timeout * 2)
        assert db1.get() == {"q": "b"}
        assert db0.unlock() is False
        assert db1.unlock() is True
        db1.delete()

    def tearDown(self):
        self.client.delete_table(TableName=self.tb)
        super().tearDown()


class TestMisc(TestCase):
    def test_dict_prod(self):
        param_dict = {"foo": [2, 3], "bar": "abc", "baz": [[5, 5], [5, 8]]}
        piter = list(dict_product(param_dict))
        total_len = reduce(lambda a, b: a * len(b), param_dict.values(), 1)
        assert len(piter) == total_len
        for k, v in param_dict.items():
            assert len([x for x in piter if x[k] == v[0]]) == total_len / len(v)
