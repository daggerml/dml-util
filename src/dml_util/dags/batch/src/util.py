import json
import logging
import os
import traceback
from dataclasses import dataclass, field
from time import time
from urllib.parse import urlparse
from uuid import uuid4

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
TIMEOUT = 5  # seconds
S3_BUCKET = os.environ["DML_S3_BUCKET"]
S3_PREFIX = os.getenv("DML_S3_PREFIX", "jobs").rstrip("/")
DYNAMO_TB = os.environ["DYNAMODB_TABLE"]
DELETE_DYNAMO_ON_FAIL = os.getenv("DELETE_DYNAMO_ON_FAIL")


def now():
    return int(time())


def get_client(name):
    logger.info("getting %r client", name)
    config = Config(connect_timeout=5, retries={"max_attempts": 0})
    return boto3.client(name, config=config)


class Error(Exception):
    def __init__(self, message, state):
        super().__init__(message)
        self.state = state


@dataclass
class S3:
    cache_key: str
    client: "boto3.client" = field(default_factory=lambda: get_client("s3"))

    @property
    def prefix(self):
        return f"{S3_PREFIX}/{self.cache_key}"

    def bucket_key(self, name):
        p = urlparse(name)
        if p.scheme == "s3":
            return p.netloc, p.path[1:]
        return S3_BUCKET, f"{self.prefix}/{name}"

    def uri(self, name):
        return f"s3://{S3_BUCKET}/{self.prefix}/{name}"

    def get(self, name):
        bucket, key = self.bucket_key(name)
        resp = self.client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read().decode()

    def put(self, data, name):
        bucket, key = self.bucket_key(name)
        self.client.put_object(Bucket=bucket, Key=key, Body=data.encode())
        return f"s3://{bucket}/{key}"

    def exists(self, name):
        bucket, key = self.bucket_key(name)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise


@dataclass
class Dynamo:
    cache_key: str
    run_id: str = field(default_factory=lambda: uuid4().hex)
    db: "boto3.client" = field(default_factory=lambda: get_client("dynamodb"))

    def _update(self, key=None, **kw):
        try:
            self.db.update_item(
                TableName=DYNAMO_TB,
                Key={"cache_key": {"S": key or self.cache_key}},
                **kw,
            )
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info("could not update %r (invalid lock)", self.cache_key)
                return False
            raise

    def put(self, obj):
        logger.info("putting data for %r", self.cache_key)
        ut = now()
        return self._update(
            UpdateExpression="SET #lk = :lk, #ut = :ut, #obj = :obj",
            ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ut < :to",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#ut": "update_time",
                "#obj": "obj",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":ut": {"N": str(ut)},
                ":to": {"N": str(ut - TIMEOUT)},
                ":obj": {"S": json.dumps(obj)},
            },
        )

    def get(self):
        logger.info("getting data for %r", self.cache_key)
        resp = self.db.get_item(TableName=DYNAMO_TB, Key={"cache_key": {"S": self.cache_key}})
        if "obj" not in resp["Item"]:
            return
        return json.loads(resp["Item"]["obj"]["S"])

    def lock(self, key=None):
        logger.info("acquiring lock for %r", self.cache_key)
        ut = now()
        return self._update(
            key,
            UpdateExpression="SET #lk = :lk, #ut = :ut",
            ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ut < :to",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#ut": "update_time",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":ut": {"N": str(ut)},
                ":to": {"N": str(ut - TIMEOUT)},
            },
        )

    def unlock(self, key=None):
        logger.info("releasing lock for %r", self.cache_key)
        return self._update(
            key,
            UpdateExpression="REMOVE #lk",
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={"#lk": "lock_key"},
            ExpressionAttributeValues={":lk": {"S": self.run_id}},
        )

    def delete(self):
        ut = now()
        return self.db.delete_item(
            TableName=DYNAMO_TB,
            Key={"cache_key": {"S": self.cache_key}},
            ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ut < :to",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#ut": "update_time",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":to": {"N": str(ut - TIMEOUT)},
            },
        )


@dataclass
class Runner:
    cache_key: str
    kwargs: "any"
    dump: str
    s3: S3 = field(init=False)

    def __post_init__(self):
        self.s3 = S3(self.cache_key)

    def fmt(self, msg):
        return f"{self.__class__.__name__.lower()} [{self.cache_key}] :: {msg}"

    def run(self):
        dynamo = Dynamo(self.cache_key)
        if not dynamo.lock():
            return {"status": 204, "message": self.fmt("Could not acquire job lock")}
        try:
            logger.info("getting dynamo info")
            info = dynamo.get()
            state, msg, dump = self.update(info)
            dynamo.delete() if state is None else dynamo.put(state)
            return {"status": 200, "message": self.fmt(msg), "dump": dump}
        except Exception as e:
            if (isinstance(e, Error) and e.state is None) or DELETE_DYNAMO_ON_FAIL:
                dynamo.delete()
            elif isinstance(e, Error):
                dynamo.put(state)
            msg = f"Error: {e}\nTraceback:\n----------\n{traceback.format_exc()}"
            return {"status": 400, "message": msg, "dump": None}
        finally:
            dynamo.unlock()
