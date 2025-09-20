"""Common test fixtures for dml-util tests."""

import logging
import os
from unittest.mock import patch

import boto3
import pytest
from daggerml import Dml

from tests.util import S3_BUCKET, S3_PREFIX

try:
    from watchtower import CloudWatchLogHandler
except ModuleNotFoundError:
    CloudWatchLogHandler = None

# Constants for testing
DYNAMO_TABLE = "test-dynamodb-table"


@pytest.fixture(scope="session")
def _aws_server():
    with patch.dict(os.environ):
        # clear out env variables for safety
        for k in os.environ:
            if k.startswith("AWS_"):
                del os.environ[k]
        from moto.server import ThreadedMotoServer

        server = ThreadedMotoServer(port=0)
        server.start()
        moto_host, moto_port = server._server.server_address
        moto_endpoint = f"http://{moto_host}:{moto_port}"
        aws_env = {
            "AWS_ACCESS_KEY_ID": "foo",
            "AWS_SECRET_ACCESS_KEY": "foo",
            "AWS_REGION": "us-east-1",
            "AWS_DEFAULT_REGION": "us-east-1",
            "AWS_ENDPOINT_URL": moto_endpoint,
        }
        try:
            yield {"server": server, "endpoint": moto_endpoint, "envvars": aws_env, "port": moto_port}
        finally:
            if CloudWatchLogHandler:
                # If watchtower is installed, we can safely remove the handler
                for name in ["dml_util", ""]:
                    logger = logging.getLogger(name)
                    for handler in (h for h in logger.handlers if isinstance(h, CloudWatchLogHandler)):
                        logger.removeHandler(handler)
                        handler.close()
            server.stop()


@pytest.fixture
def logs(_aws_server):
    """Create a mock CloudWatch Logs client for testing."""
    logs = boto3.client("logs", endpoint_url=_aws_server["endpoint"])
    logs.create_log_group(logGroupName="dml")
    try:
        yield logs
    finally:
        # list all log streams in the group and delete them
        log_streams = logs.describe_log_streams(logGroupName="dml").get("logStreams", [])
        for stream in log_streams:
            logs.delete_log_stream(logGroupName="dml", logStreamName=stream["logStreamName"])
        # delete the log group
        logs.delete_log_group(logGroupName="dml")


@pytest.fixture(autouse=True)
def clear_envvars():
    with patch.dict(os.environ):
        # Clear AWS environment variables before any tests run
        for k in os.environ:
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]
        os.environ["AWS_SHARED_CREDENTIALS_FILE"] = "/dev/null"
        os.environ["DML_S3_BUCKET"] = S3_BUCKET
        os.environ["DML_S3_PREFIX"] = S3_PREFIX
        yield


@pytest.fixture(autouse=True)
def debug(clear_envvars):
    """Fixture to set debug mode for tests."""
    with patch.dict(os.environ, {"DML_DEBUG": "1"}):
        yield


@pytest.fixture
def dml(tmpdir):
    with Dml.temporary(cache_path=str(tmpdir)) as _dml:
        with patch.dict(os.environ, _dml.envvars):
            yield _dml


@pytest.fixture
def aws_server(_aws_server, clear_envvars):
    # clear out env variables for safety
    # this loads env vars, so import after clearing
    boto3.setup_default_session()
    with patch.dict(os.environ, _aws_server["envvars"]):
        yield _aws_server
    boto3.setup_default_session()


@pytest.fixture
def s3_bucket(aws_server):
    """Create a mock S3 bucket for testing."""
    s3 = boto3.client("s3", endpoint_url=aws_server["endpoint"])
    s3.create_bucket(Bucket=os.environ["DML_S3_BUCKET"])
    yield S3_BUCKET
    # delete all objects
    for obj in s3.list_objects_v2(Bucket=S3_BUCKET).get("Contents", []):
        s3.delete_object(Bucket=S3_BUCKET, Key=obj["Key"])
    s3.delete_bucket(Bucket=S3_BUCKET)


@pytest.fixture
def dynamodb_table(aws_server):
    """Create a mock DynamoDB table for testing."""
    dynamodb = boto3.client("dynamodb", endpoint_url=aws_server["endpoint"])
    dynamodb.create_table(
        TableName=DYNAMO_TABLE,
        KeySchema=[{"AttributeName": "cache_key", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "cache_key", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )
    with patch.dict(os.environ, {"DYNAMODB_TABLE": DYNAMO_TABLE}):
        yield DYNAMO_TABLE
    dynamodb.delete_table(TableName=DYNAMO_TABLE)
