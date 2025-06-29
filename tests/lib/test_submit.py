import json
from time import sleep, time
from unittest.mock import MagicMock, patch

import boto3

from dml_util.baseutil import proc_exists
from dml_util.lib.submit import Streamer, _run_and_stream, launch_detached
from tests.test_baseutil import AwsTestCase


class TestStreamer(AwsTestCase):
    def setUp(self):
        super().setUp()
        self.client = boto3.client("logs", region_name="us-east-1", endpoint_url=self.moto_endpoint)
        self.client.create_log_group(logGroupName=self.id())

    def tearDown(self):
        self.client.delete_log_group(logGroupName=self.id())
        super().tearDown()

    def test_streamer_send_logs(self):
        fd_r = MagicMock()
        fd_r.readline.side_effect = ["log1\n", "log2\n", ""]
        streamer = Streamer(self.id(), "test-stream", fd_r, client=self.client)
        mock_put_log_events = MagicMock()
        with patch.object(self.client, "put_log_events", mock_put_log_events):
            streamer.run()
            streamer.thread.join()
        assert mock_put_log_events.call_count == 1
        messages = mock_put_log_events.call_args[1]["logEvents"]
        assert [x["message"] for x in messages[1:-1]] == ["log1", "log2"]

    def test_streamer_create_log_stream(self):
        fd_r = MagicMock()
        fd_r.readline.side_effect = ["log1\n", ""]
        streamer = Streamer(self.id(), "test-stream", fd_r, client=self.client)

        with patch.object(self.client, "create_log_stream") as mock_create_log_stream:
            streamer.run()
            mock_create_log_stream.assert_called_with(logGroupName=self.id(), logStreamName="test-stream")

    def test_run_and_stream(self):
        t0 = time() * 1000
        command = ["bash", "-c", 'for i in {1..5}; do echo "test $i"; sleep 0.1; done']
        _run_and_stream(command, self.id(), self.id(), "foo", "bar")
        t1 = time() * 1000
        logs = self.client.get_log_events(logGroupName=self.id(), logStreamName="foo")["events"]
        sleep(0.2)
        assert len(logs) == 7
        assert [x["message"] for x in logs[1:-1]] == [f"test {i}" for i in range(1, 6)]
        assert min(x["timestamp"] for x in logs) >= int(t0)
        assert max(x["timestamp"] for x in logs) < t1

    def test_launch_detached(self):
        command = ["bash", "-c", 'for i in {1..5}; do echo "test $i"; sleep 0.1; done']
        t0 = time() * 1000
        pid = launch_detached(
            command,
            {
                "DML_CMD": json.dumps(command),
                "DML_RUN_ID": "1",
                "DML_LOG_GROUP": self.id(),
                "DML_LOG_STDOUT": "foo",
                "DML_LOG_STDERR": "bar",
            },
        )
        self.assertIsInstance(pid, int)
        # poll the process to see when it finishes
        while proc_exists(pid):
            sleep(0.1)
        sleep(0.1)
        t1 = time() * 1000
        logs = self.client.get_log_events(logGroupName=self.id(), logStreamName="foo")["events"]
        assert len(logs) == 7
        assert [x["message"] for x in logs[1:-1]] == [f"test {i}" for i in range(1, 6)]
        assert min(x["timestamp"] for x in logs) > t0
        assert max(x["timestamp"] for x in logs) < t1
