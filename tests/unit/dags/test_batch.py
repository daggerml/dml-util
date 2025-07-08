"""Integration tests for the BatchRunner runner with local execution."""

import json
import os
import uuid
from dataclasses import dataclass, field
from io import StringIO
from unittest.mock import patch

import pytest

from dml_util.adapters import LambdaAdapter
from dml_util.core.config import EnvConfig
from dml_util.runners.batch import BatchRunner
from tests.util import Config


class IO(StringIO):
    def read(self):
        val = self.getvalue().strip()
        self.truncate(0)
        self.seek(0)
        return val


@dataclass
class LocalBatchClient:
    """Mock AWS Batch client that runs jobs locally."""
    job_definitions: dict = field(default_factory=dict)
    jobs: dict = field(default_factory=dict)
    running_jobs: set = field(default_factory=set)

    def register_job_definition(self, **kwargs):
        job_def_id = str(uuid.uuid4())
        job_def_arn = f"arn:aws:batch:us-east-1:123456789012:job-definition/{kwargs['jobDefinitionName']}-{job_def_id}"
        self.job_definitions[job_def_arn] = kwargs
        return {"jobDefinitionArn": job_def_arn}

    def submit_job(self, **kwargs):
        job_id = f"job-{str(uuid.uuid4())}"
        self.jobs[job_id] = {
            "jobId": job_id,
            "status": "SUBMITTED",
            "jobDefinition": kwargs["jobDefinition"],
            "jobName": kwargs["jobName"],
            "jobQueue": kwargs["jobQueue"],
            "container": {"exitCode": None},
        }
        self.running_jobs.add(job_id)
        return {"jobId": job_id}

    def describe_jobs(self, jobs):
        result = {"jobs": []}
        for job_id in jobs:
            if job_id in self.jobs:
                # If the job was added to running_jobs but hasn't been updated yet, mark as RUNNING
                if job_id in self.running_jobs:
                    self.jobs[job_id]["status"] = "RUNNING"
                result["jobs"].append(self.jobs[job_id])
        return result

    def update_job_status(self, job_id, status, exit_code=0):
        """Helper method to update job status for testing."""
        if job_id in self.jobs:
            self.jobs[job_id]["status"] = status
            self.jobs[job_id]["container"]["exitCode"] = exit_code
            if status in ("SUCCEEDED", "FAILED"):
                self.running_jobs.discard(job_id)

    def cancel_job(self, jobId, **kwargs):
        """Cancel a job."""
        if jobId in self.jobs:
            self.jobs[jobId]["status"] = "CANCELLED"
            self.running_jobs.discard(jobId)

    def deregister_job_definition(self, jobDefinition):
        """Deregister a job definition."""
        if jobDefinition in self.job_definitions:
            del self.job_definitions[jobDefinition]


def lambda_mock(fn):
    """Create a mock for Lambda invocations."""
    def inner(Payload, **kw):
        event = json.loads(Payload)
        context = kw
        return {"Payload": Config(read=lambda: json.dumps(fn(event, context)))}
    # return a mock with a `invoke` method
    inner.invoke = lambda Payload, **kw: inner(Payload, **kw)
    return inner


@pytest.fixture(autouse=True)
def batch_env(dynamodb_table):
    """Fixture to set up environment variables for testing."""
    env_vars = {
        "CPU_QUEUE": "foo",
        "GPU_QUEUE": "bar",
        "BATCH_TASK_ROLE_ARN": "arn:aws:iam::123456789012:role/BatchTaskRole",
    }
    with patch.dict(os.environ, env_vars):
        yield


@pytest.fixture
def mock_batch_client():
    """Create a mock AWS Batch client."""
    return LocalBatchClient()


@pytest.mark.usefixtures("dynamodb_table", "s3_bucket", "adapter_setup")
@patch("time.sleep", lambda _: None)
@patch("dml_util.runners.batch.BatchRunner.submit")
def test_local_batch_execution(mock_submit, mock_batch_client):
    """Test Batch runner execution with local batch client."""
    # Prepare test data
    data = {
        "cache_key": "foo:key",
        "cache_path": "bar",
        "kwargs": {
            "sub": {"uri": "bar", "data": {}, "adapter": "baz"},
            "image": {"uri": "foo:uri"},
        },
        "dump": "opaque",
    }

    conf = Config(
        uri="asdf:uri",
        input=Config(read=lambda: json.dumps(data)),
        output=IO(),
        error=IO(),
        n_iters=1,
    )

    # Set up mock_submit to return a job state
    job_id = "job-" + str(uuid.uuid4())
    job_def = "arn:aws:batch:us-east-1:123456789012:job-definition/test-job-def"
    mock_submit.return_value = {"job_def": job_def, "job_id": job_id}

    # Mock the AWS Batch and Lambda clients
    with patch.object(BatchRunner, "client", mock_batch_client), \
         patch.object(LambdaAdapter, "client", new=lambda_mock(BatchRunner.handler)):
        mock_batch_client.jobs[job_id] = {
            "jobId": job_id,
            "status": "SUBMITTED",
            "jobDefinition": job_def,
            "jobName": f"fn-{data['cache_key']}",
            "jobQueue": "cpu-queue",
            "container": {"exitCode": None},
        }
        mock_batch_client.running_jobs.add(job_id)

        # Test job submission
        status = LambdaAdapter.cli(conf)
        assert status == 0

        # Get the job_id from error output
        error_output = conf.error.read()
        assert "submitted" in error_output

        # Test job in progress
        status = LambdaAdapter.cli(conf)
        assert status == 0
        assert "RUNNING" in conf.error.read()

        # Prepare job output
        bc = BatchRunner(EnvConfig.from_env(), data)
        bc.s3.put(b"opaque-result", uri=bc.output_loc)

        # Test job completion
        mock_batch_client.update_job_status(job_id, "SUCCEEDED")
        status = LambdaAdapter.cli(conf)
        assert status == 0
        assert "SUCCEEDED" in conf.error.read()
        assert conf.output.read() == "opaque-result"


@pytest.mark.usefixtures("dynamodb_table", "s3_bucket", "adapter_setup")
@patch("time.sleep", lambda _: None)
@patch("dml_util.runners.batch.BatchRunner.submit")
def test_local_batch_failed_job(mock_submit, mock_batch_client):
    """Test Batch runner with a failed job."""
    # Prepare test data
    data = {
        "cache_key": "foo:key",
        "cache_path": "bar",
        "kwargs": {
            "sub": {"uri": "bar", "data": {}, "adapter": "baz"},
            "image": {"uri": "foo:uri"},
        },
        "dump": "opaque",
    }

    conf = Config(
        uri="asdf:uri",
        input=Config(read=lambda: json.dumps(data)),
        output=IO(),
        error=IO(),
        n_iters=1,
    )

    # Set up mock_submit to return a job state
    job_id = "job-" + str(uuid.uuid4())
    job_def = "arn:aws:batch:us-east-1:123456789012:job-definition/test-job-def"
    mock_submit.return_value = {"job_def": job_def, "job_id": job_id}

    # Mock the AWS BatchRunner and Lambda clients
    with patch.object(BatchRunner, "client", mock_batch_client), \
         patch.object(LambdaAdapter, "client", new=lambda_mock(BatchRunner.handler)):
        # Add the job to the mock batch client
        mock_batch_client.jobs[job_id] = {
            "jobId": job_id,
            "status": "SUBMITTED",
            "jobDefinition": job_def,
            "jobName": f"fn-{data['cache_key']}",
            "jobQueue": "cpu-queue",
            "container": {"exitCode": None},
        }
        mock_batch_client.running_jobs.add(job_id)

        # Test job submission
        status = LambdaAdapter.cli(conf)
        assert status == 0

        # Get the job_id from error output
        conf.error.read()

        # Test job failure
        mock_batch_client.update_job_status(job_id, "FAILED", exit_code=1)

        # Write error message to S3
        bc = BatchRunner(EnvConfig.from_env(), data)
        bc.s3.put(b"Job failed with error", name="error.dump")

        # Test job completion - should raise an exception due to failure
        resp = LambdaAdapter.cli(conf)
        assert resp == 1
        err = conf.error.read()
        assert err.startswith("lambda returned with bad status: 400")
        assert "FAILED" in err


@pytest.mark.usefixtures("dynamodb_table", "s3_bucket", "adapter_setup")
@patch("time.sleep", lambda _: None)
@patch("dml_util.runners.batch.BatchRunner.submit")
def test_local_batch_no_output(mock_submit, mock_batch_client):
    """Test Batch runner with a job that completes but has no output."""
    # Prepare test data
    data = {
        "cache_key": "foo:key",
        "cache_path": "bar",
        "kwargs": {
            "sub": {"uri": "bar", "data": {}, "adapter": "baz"},
            "image": {"uri": "foo:uri"},
        },
        "dump": "opaque",
    }

    conf = Config(
        uri="asdf:uri",
        input=Config(read=lambda: json.dumps(data)),
        output=IO(),
        error=IO(),
        n_iters=1,
    )

    # Set up mock_submit to return a job state
    job_id = "job-" + str(uuid.uuid4())
    job_def = "arn:aws:batch:us-east-1:123456789012:job-definition/test-job-def"
    mock_submit.return_value = {"job_def": job_def, "job_id": job_id}
    # Mock the AWS BatchRunner and Lambda clients
    with patch.object(BatchRunner, "client", mock_batch_client), \
         patch.object(LambdaAdapter, "client", new=lambda_mock(BatchRunner.handler)):
        mock_batch_client.jobs[job_id] = {
            "jobId": job_id,
            "status": "SUBMITTED",
            "jobDefinition": job_def,
            "jobName": f"fn-{data['cache_key']}",
            "jobQueue": "cpu-queue",
            "container": {"exitCode": None},
        }
        mock_batch_client.running_jobs.add(job_id)

        # Test job submission
        status = LambdaAdapter.cli(conf)
        assert status == 0

        # Get the job_id from error output
        conf.error.read()

        # Set job as succeeded but don't write output
        mock_batch_client.update_job_status(job_id, "SUCCEEDED")

        # Should fail due to missing output
        assert LambdaAdapter.cli(conf) == 1

        error_msg = conf.error.read()
        assert "SUCCEEDED" in error_msg
        assert "no output" in error_msg
