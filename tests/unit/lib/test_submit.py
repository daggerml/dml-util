import json
from time import sleep, time
from unittest.mock import MagicMock, patch

import boto3
import pytest
from botocore.exceptions import NoCredentialsError, NoRegionError

from dml_util.core.utils import proc_exists
from dml_util.lib.submit import Streamer, launch_detached

LOG_GROUP_NAME = "test-log-group"
LOG_STREAM_NAME = "test-log-stream"

@pytest.fixture
def log_group(aws_server):
    """Fixture to create a log group for testing."""
    client = boto3.client("logs", endpoint_url=aws_server["endpoint"])
    client.create_log_group(logGroupName=LOG_GROUP_NAME)
    yield LOG_GROUP_NAME
    client.delete_log_group(logGroupName=LOG_GROUP_NAME)


@pytest.fixture
def log_stream(log_group):
    """Fixture to create a log stream for testing."""
    client = boto3.client("logs")
    client.create_log_stream(logGroupName=log_group, logStreamName=LOG_STREAM_NAME)
    yield LOG_STREAM_NAME
    client.delete_log_stream(logGroupName=log_group, logStreamName=LOG_STREAM_NAME)


@pytest.mark.usefixtures("log_stream")
def test_streamer_send_logs():
    """Test that the Streamer sends logs to CloudWatch."""
    client = boto3.client("logs")
    fd_r = MagicMock()
    fd_r.readline.side_effect = ["log1\n", "log2\n", ""]
    streamer = Streamer(LOG_GROUP_NAME, LOG_STREAM_NAME, fd_r, client=client)
    streamer.run()
    streamer.thread.join()
    logs = client.get_log_events(logGroupName=LOG_GROUP_NAME, logStreamName=LOG_STREAM_NAME)["events"]
    assert [x["message"] for x in logs[1:-1]] == ["log1", "log2"]


def test_fails_gracefully():
    try:
        sts_client = boto3.client("sts")
    except NoRegionError:
        sts_client = boto3.client("sts", region_name="us-west-2")
    with pytest.raises(NoCredentialsError):
        sts_client.get_caller_identity()
    fd_r = MagicMock()
    fd_r.readline.side_effect = ["log1\n", "log2\n", ""]
    streamer = Streamer(LOG_GROUP_NAME, LOG_STREAM_NAME, fd_r)
    with patch.object(streamer.client, "put_log_events") as mock_client:
        with patch("dml_util.lib.submit.logger.warning") as mock_warning:
            streamer.run()
            streamer.join()
            mock_warning.assert_called_with(
                f"*** No CloudWatch client available for {streamer.run_id} ***"
            )
        mock_client.put_log_events.assert_not_called()

@pytest.mark.usefixtures("log_group")
def test_launch_detached():
    command = ["bash", "-c", 'for i in {1..5}; do echo "test $i"; sleep 0.1; done']
    t0 = time() * 1000
    pid = launch_detached(
        command,
        {
            "DML_CMD": json.dumps(command),
            "DML_RUN_ID": "1",
            "DML_LOG_GROUP": LOG_GROUP_NAME,
            "DML_LOG_STDOUT": "foo",
            "DML_LOG_STDERR": "bar",
        },
    )
    assert isinstance(pid, int)
    while proc_exists(pid):
        sleep(0.1)
    sleep(0.1)
    t1 = time() * 1000
    logs = boto3.client("logs").get_log_events(logGroupName=LOG_GROUP_NAME, logStreamName="foo")["events"]
    assert len(logs) == 7
    assert [x["message"] for x in logs[1:-1]] == [f"test {i}" for i in range(1, 6)]
    assert min(x["timestamp"] for x in logs) > t0
    assert max(x["timestamp"] for x in logs) < t1
