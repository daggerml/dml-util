# TODO:  should install dml-util and use it directly
import logging
import os
from textwrap import dedent

from baseutil import S3_BUCKET, S3_PREFIX, DagRunError, LambdaRunner, get_client
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
CPU_QUEUE = os.environ["CPU_QUEUE"]
GPU_QUEUE = os.environ["GPU_QUEUE"]
DFLT_PROP = {"vcpus": 1, "memory": 512}
PENDING_STATES = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]
SUCCESS_STATE = "SUCCEEDED"
FAILED_STATE = "FAILED"


class Batch(LambdaRunner):
    client = get_client("batch")

    def submit(self):
        image = self.kwargs.pop("image")
        script = self.kwargs.pop("script")
        container_props = DFLT_PROP
        container_props.update(self.kwargs)
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
                        "value": self.s3.put(self.dump.encode(), name="input.dump"),
                    },
                    {
                        "name": "DML_OUTPUT_LOC",
                        "value": self.s3.name2uri("output.dump"),
                    },
                    {
                        "name": "DML_S3_BUCKET",
                        "value": S3_BUCKET,
                    },
                    {
                        "name": "DML_S3_PREFIX",
                        "value": S3_PREFIX,
                    },
                    {
                        "name": "DML_CACHE_KEY",
                        "value": self.cache_key,
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

    def finish(self, state, msg):
        job_id = "???"
        if state:
            self.gc(state)
            job_id = state["job_id"]
        if not self.s3.exists("output.dump"):
            raise DagRunError(f"{job_id = } {msg} : no output", None)
        dump = self.s3.get("output.dump")
        msg = f"{job_id = } {msg}."
        return None, msg, dump

    def describe_job(self, state):
        job_id = state["job_id"]
        response = self.client.describe_jobs(jobs=[job_id])
        logger.info("Job %r (cache_key: %r) description: %r", job_id, self.cache_key, response)
        if len(response) == 0:
            return None, None
        job = response["jobs"][0]
        status = job["status"]
        return job_id, status

    def update(self, state):
        if state == {}:
            state = self.submit()
            job_id = state["job_id"]
            return state, f"{job_id = } submitted", None
        job_id, status = self.describe_job(state)
        if status in [SUCCESS_STATE, FAILED_STATE, None]:
            return self.finish(state, f"finished with status {status}")
        msg = f"{job_id = } {status}"
        return state, msg, None

    def gc(self, state):
        job_id, status = self.describe_job(state)
        try:
            self.client.cancel_job(jobId=job_id, reason="cancellation")
        except ClientError as e:
            pass
        job_def = state["job_def"]
        try:
            self.client.deregister_job_definition(jobDefinition=job_def)
            logger.info("Successfully deregistered: %r", job_def)
            return
        except ClientError as e:
            if e.response.get("Error", {}).get("Code") != "ClientException":
                raise
            if "DEREGISTERED" not in e.response.get("Error", {}).get("Message"):
                raise

    def delete(self, state):
        self.gc(state)
        super().delete(state)


handler = Batch.handler
