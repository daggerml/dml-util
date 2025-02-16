import json
import logging
import os
from dataclasses import dataclass, field
from textwrap import dedent

from botocore.exceptions import ClientError
from dml_lambda.util import S3_BUCKET, Runner, get_client

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
CPU_QUEUE = os.environ["CPU_QUEUE"]
GPU_QUEUE = os.environ["GPU_QUEUE"]
DFLT_PROP = {"vcpus": 1, "memory": 512}
PENDING_STATES = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]
SUCCESS_STATE = "SUCCEEDED"
FAILED_STATE = "FAILED"


@dataclass
class Batch(Runner):
    client = field(default_factory=lambda: get_client("batch"))

    def submit(self):
        image = self.kwargs.pop("image")[-1]
        script = self.kwargs.pop("script")[-1]
        container_props = {**DFLT_PROP, **self.kwargs}
        needs_gpu = any(x["type"] == "GPU" for x in container_props.get("resourceRequirements", []))
        logger.info("createing job definition with name: %r", f"fn-{self.cache_key}")
        response = self.client.register_job_definition(
            jobDefinitionName=f"fn-{self.cache_key}",
            type="container",
            containerProperties={
                "image": image,
                "command": [
                    "python3",
                    "-c",
                    dedent(script).strip(),
                ],
                "environment": [
                    {
                        "name": "DML_INPUT_LOC",
                        "value": self.s3.put(self.dump, "input.dump"),
                    },
                    {
                        "name": "DML_OUTPUT_LOC",
                        "value": self.s3.uri("output.dump"),
                    },
                    {
                        "name": "DML_S3_BUCKET",
                        "value": S3_BUCKET,
                    },
                    {
                        "name": "DML_S3_PREFIX",
                        "value": self.s3.prefix,
                    },
                ],
                "jobRoleArn": os.environ["BATCH_TASK_ROLE_ARN"],
                **container_props,
            },
        )
        job_def = response["jobDefinitionArn"]
        logger.info("created job definition with arn: %r", job_def)
        response = self.client.submit_job(
            jobName=f"fn-{self.cache_key}",
            jobQueue=GPU_QUEUE if needs_gpu else CPU_QUEUE,
            jobDefinition=job_def,
        )
        logger.info("Job submitted: %r", response["jobId"])
        job_id = response["jobId"]
        return {"job_def": job_def, "job_id": job_id}

    def update(self, state):
        if state is None:
            state = self.submit()
            job_id = state["job_id"]
            return state, f"{job_id = } submitted", None
        job_id = state["job_id"]
        response = self.client.describe_jobs(jobs=[job_id])
        job = response["jobs"][0]
        status = job["status"]
        if status == SUCCESS_STATE:
            self.gc(state["job_def"])
            if self.s3.exists("output.dump"):
                dump = self.s3.get("output.dump")
                return None, f"{job_id = } succeeded.", dump
            msg = f"Job {job_id} succeeded but did not write output."
            raise RuntimeError(msg)
        if status == FAILED_STATE:
            self.gc(state["job_def"])
            failure_reason = job.get("attempts", [{}])[-1].get("container", {}).get("reason", "Unknown failure reason")
            status_reason = job.get("statusReason", "No additional status reason provided.")
            msg = json.dumps(
                {
                    "job_id": job_id,
                    "cache_key": self.cache_key,
                    "failure_reason": failure_reason,
                    "status_reason": status_reason,
                },
                separators=(",", ":"),
            )
            raise RuntimeError(msg)
        msg = f"{job_id = } {status}"
        return state, msg, None

    def gc(self, job_def):
        try:
            self.client.deregister_job_definition(jobDefinition=job_def)
            logger.info("Successfully deregistered: %r", job_def)
            return
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ClientException":
                raise
            if "DEREGISTERED" not in e.response.get("Error", {}).get("Message"):
                raise
