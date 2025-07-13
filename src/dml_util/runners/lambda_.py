"""Lambda Runner."""

import json
import logging
import traceback
from dataclasses import dataclass

from dml_util.aws.dynamodb import DynamoState
from dml_util.aws.s3 import S3Store
from dml_util.runners.base import RunnerBase

logger = logging.getLogger(__name__)


@dataclass
class LambdaRunner(RunnerBase):
    """Runner for AWS Lambda functions."""

    state_class = DynamoState

    def __post_init__(self):
        super().__post_init__()
        self.s3 = S3Store(bucket=self.config.s3_bucket, prefix=f"{self.prefix}/jobs/{self.input.cache_key}")
        logger.info(f"Initialized LambdaRunner. Writing execution data to s3://{self.s3.bucket}/{self.s3.prefix}")

    @property
    def output_loc(self):
        return self.s3._name2uri("output.dump")

    @classmethod
    def handler(cls, event, context):
        try:
            event["input"] = json.loads(event.pop("dump"))
            logger.info(f"Lambda event: {json.dumps(event)}")
            response, msg = cls(**event).run()
            status = 200 if response else 201
            return {"status": status, "response": response, "message": msg}
        except Exception as e:
            msg = f"Error in lambda: {e}\n\n{traceback.format_exc()}"
            return {"status": 400, "response": {}, "message": msg}

    def gc(self, state):
        """Garbage collect resources used by the runner."""
        logger.info("Cleaning up resources for key: %s", self.input.cache_key)
        objs = self.s3.ls(recursive=True)
        if objs:
            logger.info("Found %d objects to clean up in S3: s3://%s/%s/", len(objs), self.s3.bucket, self.s3.prefix)
            self.s3.rm(*objs)
        logger.info("Cleanup completed for key: %s", self.input.cache_key)
