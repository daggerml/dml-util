import os
from itertools import product
from unittest.mock import MagicMock, call, patch

import pytest

from dml_util.aws import get_client
from dml_util.core.config import EnvConfig, InputConfig
from dml_util.runners.batch import FAILED_STATE, PENDING_STATES, SUCCESS_STATE, BatchRunner


@pytest.fixture(autouse=True)
def add_envvars():
    with patch.dict(os.environ):
        os.environ.update({"CPU_QUEUE": "cpu-q", "GPU_QUEUE": "gpu-q", "BATCH_TASK_ROLE_ARN": "arn"})
        yield


@pytest.fixture
def batch_runner():
    runner = BatchRunner(
        EnvConfig.from_env(debug=True),
        InputConfig(
            cache_path="/dev/null",
            cache_key="test-key",
            kwargs={
                "spec": {"resourceRequirements": [{"type": "MEMORY", "value": 1024}]},
                "image": {"uri": "test-image"},
                "memory_max": 2048,
                "sub": {
                    "adapter": "adapter",
                    "uri": "uri",
                    "data": {"foo": "bar"},
                },
            },
            dump="{}",
        ),
    )
    runner._client = get_client("batch")
    runner._client = MagicMock(auto_spec=True)
    _s3 = runner.s3
    runner.s3 = MagicMock(auto_spec=True)
    runner.s3.bucket = _s3.bucket
    runner.s3.prefix = _s3.prefix
    runner.s3._name2uri = _s3._name2uri
    return runner


def test_update_no_state(batch_runner):
    job_id, job_def = "job-123", "arn:job-def"
    batch_runner._client.register_job_definition.return_value = {"jobDefinitionArn": job_def}
    batch_runner._client.submit_job.return_value = {"jobId": job_id}
    state, msg, dump = batch_runner.update({})
    assert not dump
    assert state == {"job_id": job_id, "job_def": job_def}
    assert "submitted" in msg
    batch_runner._client.register_job_definition.assert_called_once()
    batch_runner._client.submit_job.assert_called_once()


@pytest.mark.parametrize("status", PENDING_STATES)
def test_update_pending_states(batch_runner, status):
    job_id, job_def = "job-123", "arn:job-def"
    state = {"job_id": job_id, "job_def": job_def}
    batch_runner._client.describe_jobs.return_value = {
        "jobs": [{"jobId": job_id, "status": status, "attempts": [{"container": {"exitCode": 0}}]}]
    }
    new_state, msg, dump = batch_runner.update(state.copy())
    assert new_state == state
    assert not dump
    assert status in msg
    batch_runner._client.describe_jobs.assert_called_once()
    assert not batch_runner._client.register_job_definition.called
    assert not batch_runner._client.submit_job.called


@pytest.mark.parametrize("is_success,has_err,has_out", product([True, False], repeat=3))
def test_update_finished(batch_runner, is_success, has_err, has_out):
    # mock the s3store (batch_runner.s3) "exists" function with another that returns True iff has_[x]
    state = {"job_id": "foo", "job_def": "bar"}

    def s3_exists(name):
        return (has_err and name == "error.dump") or (has_out and name == "output.dump")

    batch_runner.s3.exists = s3_exists
    batch_runner.s3.get.return_value = b"testing"
    batch_runner._client.describe_jobs.return_value = {
        "jobs": [{"jobId": state["job_id"], "status": SUCCESS_STATE if is_success else FAILED_STATE}]
    }
    if is_success and has_out:
        new_state, msg, dump = batch_runner.update(state)
        assert new_state is None
        assert SUCCESS_STATE in msg
        assert dump
    else:
        with pytest.raises(RuntimeError) as excinfo:
            batch_runner.update(state)
        if is_success:
            assert "no output found" in str(excinfo.value)
        if has_err:
            assert "testing" in str(excinfo.value)
    assert not batch_runner._client.register_job_definition.called
    assert not batch_runner._client.submit_job.called


def test_gc(batch_runner):
    # mock the s3store (batch_runner.s3) "exists" function with another that returns True iff has_[x]
    state = {"job_id": "foo", "job_def": "bar"}

    vals = list("abc")
    batch_runner.s3.ls.return_value = vals
    batch_runner._client.describe_jobs.return_value = {"jobs": [{"jobId": state["job_id"], "status": FAILED_STATE}]}
    batch_runner.gc(state)
    assert not batch_runner._client.register_job_definition.called
    assert not batch_runner._client.submit_job.called
    batch_runner._client.deregister_job_definition.assert_called_once()
    batch_runner.s3.rm.assert_called_once()
    assert batch_runner.s3.rm.call_args_list == [call(*vals)]
