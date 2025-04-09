import json
import logging
import os

from baseutil import LambdaRunner, get_client
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
        sub_uri, sub_kwargs, sub_adapter = self._to_data()
        print(f"{sub_uri = }")
        print(f"{sub_kwargs = }")
        print(f"{sub_adapter = }")
        kw = self.kwargs.copy()
        kw.pop("sub")
        image = kw.pop("image")["uri"]
        container_props = DFLT_PROP
        container_props.update(kw)
        needs_gpu = any(x["type"] == "GPU" for x in container_props.get("resourceRequirements", []))
        logger.info("createing job definition with name: %r", f"fn-{self.cache_key}")
        response = self.client.register_job_definition(
            jobDefinitionName=f"fn-{self.cache_key}",
            type="container",
            containerProperties={
                "image": image,
                "command": [
                    sub_adapter,
                    "-d",
                    "-i",
                    self.s3.put(sub_kwargs.encode(), name="input.dump"),
                    "-o",
                    self.s3.name2uri("output.dump"),
                    "-e",
                    self.s3.name2uri("error.dump"),
                    sub_uri,
                ],
                "environment": [
                    *[{"name": k, "value": v} for k, v in self.env.items()],
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

    def describe_job(self, state):
        job_id = state["job_id"]
        response = self.client.describe_jobs(jobs=[job_id])
        logger.info("Job %r (cache_key: %r) description: %r", job_id, self.cache_key, response)
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
            logger.info("%r found with content: %r", self.s3.name2uri("error.dump"), err)
            msg += f"\n\n{err}"
        if status == SUCCESS_STATE and self.s3.exists("output.dump"):
            logger.info("job finished successfully and output was written...")
            js = self.s3.get_js("output.dump")
            logger.info("dump = %r", js)
            return state, msg, js
        logger.info("file: %r does not exist", self.s3.name2uri("output.dump"))
        msg = json.dumps(
            {
                "job_id": job_id,
                "message": msg,
                "status_reason": ("status is none" if status is None else self.job_desc["statusReason"]),
            }
        )
        logger.info(msg)
        raise RuntimeError(f"{msg = }")

    def gc(self, state):
        if state is not None and len(state) > 0:
            job_id, status = self.describe_job(state)
            try:
                self.client.cancel_job(jobId=job_id, reason="gc")
            except ClientError:
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
            self.s3.rm(*self.s3.ls(recursive=True))
        super().gc(state)


handler = Batch.handler
