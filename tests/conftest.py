"""Common test fixtures for dml-util tests."""

import logging
import os
import sys
from io import StringIO
from unittest.mock import patch

import boto3
import pytest

from dml_util.adapters import AdapterBase
from dml_util.core.config import EnvConfig
from tests.util import S3_BUCKET, S3_PREFIX, tmpdir

try:
    from watchtower import CloudWatchLogHandler
except ModuleNotFoundError:
    CloudWatchLogHandler = None

# Constants for testing
CACHE_PATH = "/tmp/cache"
CACHE_KEY = "test_key"
TEST_RUN_ID = "test-run-id"
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
            yield {"server": server, "endpoint": moto_endpoint, "envvars": aws_env}
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
def clear_envvars():
    with patch.dict(os.environ):
        # Clear AWS environment variables before any tests run
        for k in os.environ:
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]
        yield

@pytest.fixture(autouse=True)
def setup_environment(clear_envvars):
    """Set up test environment variables.

    This fixture sets up common environment variables needed by many tests.
    It also restores the original environment after the test.
    """
    with patch.dict("os.environ"):
        for k in os.environ:
            if k.startswith("DML_"):
                del os.environ[k]
        os.environ["DML_CACHE_PATH"] = CACHE_PATH
        os.environ["DML_CACHE_KEY"] = CACHE_KEY
        os.environ["DML_S3_BUCKET"] = S3_BUCKET
        os.environ["DML_S3_PREFIX"] = S3_PREFIX
        os.environ["DML_RUN_ID"] = TEST_RUN_ID
        with tmpdir() as tmpd:
            os.environ["DML_FN_CACHE_DIR"] = tmpd
            yield


@pytest.fixture
def aws_server(_aws_server, clear_envvars):
    # clear out env variables for safety
    # this loads env vars, so import after clearing
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


@pytest.fixture
def test_config():
    """Return a test configuration object."""
    return EnvConfig.from_env(debug=False)


@pytest.fixture
def io_capture():
    """Capture stdout and stderr for testing.

    This fixture redirects stdout and stderr to StringIO objects for capturing
    and testing output. It restores the original stdout and stderr after the test.

    Returns
    -------
    tuple
        (stdout_capture, stderr_capture) as StringIO objects
    """
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    stdout_capture = StringIO()
    stderr_capture = StringIO()
    sys.stdout = stdout_capture
    sys.stderr = stderr_capture
    yield stdout_capture, stderr_capture
    sys.stdout = original_stdout
    sys.stderr = original_stderr

@pytest.fixture
def adapter_setup():
    """Patch the setup method of all adapters."""
    with patch.object(AdapterBase, "_setup", return_value=None) as mock_setup:
        yield mock_setup
