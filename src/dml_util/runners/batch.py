"""
Implementation of a Lambda function that runs a job on AWS Batch.

Environment variables:
- CPU_QUEUE: The name of the CPU job queue.
- GPU_QUEUE: The name of the GPU job queue.
- BATCH_TASK_ROLE_ARN: The ARN of the IAM role for Batch tasks.
"""
import logging
import os
from typing import TYPE_CHECKING, Optional

from botocore.exceptions import ClientError

from dml_util.aws import get_client
from dml_util.runners.lambda_ import LambdaRunner

if TYPE_CHECKING:
    import boto3

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
DFLT_PROP = {"vcpus": 1, "memory": 512}
PENDING_STATES = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]
SUCCESS_STATE = "SUCCEEDED"
FAILED_STATE = "FAILED"


class BatchRunner(LambdaRunner):
    _client: Optional["boto3.client"] = None

    @property
    def client(self):
        """Return the AWS Batch client."""
        if self._client is None:
            self._client = get_client("batch")
        return self._client

    def submit(self):
        sub_adapter, sub_uri, sub_kwargs = self.input.get_sub()
        kw = self.input.kwargs.copy()
        kw.pop("sub")
        image = kw.pop("image")["uri"]
        container_props = DFLT_PROP
        container_props.update(kw)
        needs_gpu = any(x["type"] == "GPU" for x in container_props.get("resourceRequirements", []))
        logger.info("createing job definition with name: %r", f"fn-{self.input.cache_key}")
        response = self.client.register_job_definition(
            jobDefinitionName=f"fn-{self.input.cache_key}",
            type="container",
            containerProperties={
                "image": image,
                "command": [
                    sub_adapter,
                    "-n",
                    "-1",
                    "-i",
                    self.s3.put(sub_kwargs.encode(), name="input.dump").uri,
                    "-o",
                    self.s3._name2uri("output.dump"),
                    "-e",
                    self.s3._name2uri("error.dump"),
                    sub_uri,
                ],
                "environment": [
                    *[{"name": k, "value": v} for k, v in self.config.to_envvars().items()],
                ],
                "jobRoleArn": os.environ["BATCH_TASK_ROLE_ARN"],
                **container_props,
            },
        )
        job_def = response["jobDefinitionArn"]
        logger.info("created job definition with arn: %r", job_def)
        response = self.client.submit_job(
            jobName=f"fn-{self.input.cache_key}",
            jobQueue=os.environ["GPU_QUEUE" if needs_gpu else "CPU_QUEUE"],
            jobDefinition=job_def,
        )
        logger.info("Job submitted: %r", response["jobId"])
        job_id = response["jobId"]
        return {"job_def": job_def, "job_id": job_id}

    def describe_job(self, state):
        job_id = state["job_id"]
        response = self.client.describe_jobs(jobs=[job_id])
        logger.info(
            "Job %r (input.cache_key: %r) description: %r",
            job_id,
            self.input.cache_key,
            response,
        )
        if len(response) == 0:
            return None, None
        job = response["jobs"][0]
        self.job_desc = job
        status = job["status"]
        return job_id, status

    def update(self, state):
        if state == {}:
            state = self.submit()
            job_id = state["job_id"]
            return state, f"{job_id = } submitted", {}
        job_id, status = self.describe_job(state)
        msg = f"{job_id = } {status}"
        logger.info(msg)
        if status in PENDING_STATES:
            return state, msg, {}
        if self.s3.exists("error.dump"):
            err = self.s3.get("error.dump").decode()
            logger.info("%r found with content: %r", self.s3._name2uri("error.dump"), err)
            msg += f"\n\n{err}"
        if status == SUCCESS_STATE and self.s3.exists("output.dump"):
            logger.info("job finished successfully and output was written...")
            js = self.s3.get("output.dump").decode()
            logger.info("dump = %r", js)
            return None, msg, js
        if not self.s3.exists("output.dump"):
            msg = f"{msg} (no output found)"
        logger.info("file: %r does not exist", self.s3._name2uri("output.dump"))
        if "statusReason" in self.job_desc:
            msg = f"{msg} (reason: {self.job_desc['statusReason']})"
        logger.info(msg)
        raise RuntimeError(f"{msg = }")

    def gc(self, state):
        super().gc(state)
        if state:
            job_id, status = self.describe_job(state)
            try:
                self.client.cancel_job(jobId=job_id, reason="gc")
            except ClientError:
                pass
            job_def = state["job_def"]
            try:
                self.client.deregister_job_definition(jobDefinition=job_def)
                logger.info("Successfully deregistered: %r", job_def)
            except ClientError as e:
                if e.response.get("Error", {}).get("Code") != "ClientException":
                    raise
                if "DEREGISTERED" not in e.response.get("Error", {}).get("Message"):
                    raise
