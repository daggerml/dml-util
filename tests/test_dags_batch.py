import json
import os
from io import StringIO
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from dml_util.adapter import LambdaAdapter
from dml_util.baseutil import get_client
from tests.test_all import FullDmlTestCase

try:
    import docker  # noqa: F401
except ImportError:
    docker = None
_root_ = Path(__file__).parent.parent


class Config:
    def __init__(self, **kwargs):
        self.__dict__.update(**kwargs)

    def __getattr__(self, item):
        return self.__dict__.get(item, None)


def lambda_mock(fn):
    def inner(Payload, **kw):
        event = json.loads(Payload)
        context = kw
        return {"Payload": Config(read=lambda: json.dumps(fn(event, context)))}

    return inner


class IO(StringIO):
    def read(self):
        val = self.getvalue().strip()
        self.truncate(0)
        self.seek(0)
        return val


class TestDagBatch(FullDmlTestCase):
    def setUp(self):
        super().setUp()
        self.dynamodb_client = get_client("dynamodb")
        self.dynamodb_table = f"test-{uuid4()}"
        os.environ["DYNAMODB_TABLE"] = self.dynamodb_table
        # create a DynamoDB table for testing with schema inferred from `DynamoState` class
        self.dynamodb_client.create_table(
            TableName=self.dynamodb_table,
            KeySchema=[
                {"AttributeName": "cache_key", "KeyType": "HASH"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "cache_key", "AttributeType": "S"},
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
        )
        os.environ["CPU_QUEUE"] = "foo"
        os.environ["GPU_QUEUE"] = "bar"
        os.environ["BATCH_TASK_ROLE_ARN"] = "arn:aws:iam::123456789012:role/BatchTaskRole"

    def test_lambda_adapter(self):
        resp = {"logs": "qwer"}

        def foo(data, _):
            return {"status": 200, "response": resp, "message": "ok"}

        conf = Config(
            uri="asdf:uri",
            input=Config(read=lambda: json.dumps({"foo": "bar"})),
            output=IO(),
            error=IO(),
            n_iters=1,
        )
        with patch.object(LambdaAdapter.CLIENT, "invoke", new=lambda_mock(foo)):
            assert LambdaAdapter.cli(conf) == 0
            assert conf.output.read() == ""
            assert conf.error.read() == "ok"
            resp = {"dump": None}
            assert LambdaAdapter.cli(conf) == 0
            assert conf.output.read() == ""
            assert conf.error.read() == "ok"
            resp = {"dump": "opaque-dump"}
            assert LambdaAdapter.cli(conf) == 0
            assert json.loads(conf.output.read()) == resp
            assert conf.error.read() == "ok"

    def test_aws_batch(self):
        from dml_util.dags.batch.src.impl import Batch

        data = {
            "cache_key": "foo:key",
            "kwargs": {
                "sub": {"uri": "bar", "data": {}, "adapter": "baz"},
                "image": {"uri": "foo:uri"},
            },
            "dump": "opaque",
            "retry": False,
        }
        conf = Config(
            uri="asdf:uri",
            input=Config(read=lambda: json.dumps(data)),
            output=IO(),
            error=IO(),
            n_iters=1,
        )

        job_def_arn = "arn:aws:batch:us-east-1:123456789012:job-definition:example-job-definition"
        job_id = f"job-id-{uuid4()}"

        with patch.object(LambdaAdapter.CLIENT, "invoke", new=lambda_mock(Batch.handler)):
            with (
                patch.object(
                    Batch.CLIENT,
                    "register_job_definition",
                    return_value={"jobDefinitionArn": job_def_arn},
                ),
                patch.object(Batch.CLIENT, "submit_job", return_value={"jobId": job_id}),
            ):
                status = LambdaAdapter.cli(conf)
                assert conf.error.read() == f"Batch [foo:key] :: job_id = {job_id!r} submitted"
                assert conf.output.read() == ""
                assert status == 0
            describe_result = {
                "jobs": [
                    {
                        "jobId": job_id,
                        "status": "RUNNING",
                        "container": {"exitCode": 0},
                    }
                ]
            }
            with patch.object(Batch.CLIENT, "describe_jobs", return_value=describe_result):
                status = LambdaAdapter.cli(conf)
                assert conf.error.read() == f"Batch [foo:key] :: job_id = {job_id!r} RUNNING"
                assert conf.output.read() == ""
                assert status == 0
            describe_result = {
                "jobs": [
                    {
                        "jobId": job_id,
                        "status": "SUCCEEDED",
                        "container": {"exitCode": 0},
                    }
                ]
            }
            bc = Batch(**data)
            bc.s3.put_js({"dump": "opaque"}, uri=bc.output_loc)
            with patch.object(Batch.CLIENT, "describe_jobs", return_value=describe_result):
                status = LambdaAdapter.cli(conf)
                assert status == 0
                assert conf.error.read() == f"Batch [foo:key] :: job_id = {job_id!r} SUCCEEDED"
                assert conf.output.read() != ""

    def test_aws_batch_no_output(self):
        from dml_util.dags.batch.src.impl import Batch

        data = {
            "cache_key": "foo:key",
            "kwargs": {
                "sub": {"uri": "bar", "data": {}, "adapter": "baz"},
                "image": {"uri": "foo:uri"},
            },
            "dump": "opaque",
            "retry": False,
        }
        conf = Config(
            uri="asdf:uri",
            input=Config(read=lambda: json.dumps(data)),
            output=IO(),
            error=IO(),
            n_iters=1,
        )

        job_def_arn = "arn:aws:batch:us-east-1:123456789012:job-definition:example-job-definition"
        job_id = f"job-id-{uuid4()}"

        with patch.object(LambdaAdapter.CLIENT, "invoke", new=lambda_mock(Batch.handler)):
            with (
                patch.object(
                    Batch.CLIENT,
                    "register_job_definition",
                    return_value={"jobDefinitionArn": job_def_arn},
                ),
                patch.object(Batch.CLIENT, "submit_job", return_value={"jobId": job_id}),
            ):
                status = LambdaAdapter.cli(conf)
                assert conf.error.read() == f"Batch [foo:key] :: job_id = {job_id!r} submitted"
                assert conf.output.read() == ""
                assert status == 0
            # exited without writing output
            describe_result = {
                "jobs": [
                    {
                        "jobId": job_id,
                        "status": "SUCCEEDED",
                        "container": {"exitCode": 0},
                    }
                ]
            }
            with patch.object(Batch.CLIENT, "describe_jobs", return_value=describe_result):
                status = LambdaAdapter.cli(conf)
                assert status == 1
                assert conf.error.read() == "lambda returned with bad status"
                assert conf.output.read() == ""
