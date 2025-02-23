import hashlib
import json
import logging
import os
import subprocess
import traceback
from dataclasses import dataclass, field
from io import BytesIO
from tempfile import NamedTemporaryFile
from time import time
from urllib.parse import urlparse
from uuid import uuid4

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError

try:
    from daggerml import Resource
except ImportError:
    Resource = str

logger = logging.getLogger(__name__)
TIMEOUT = 5  # seconds
S3_BUCKET = os.environ["DML_S3_BUCKET"]
S3_PREFIX = os.getenv("DML_S3_PREFIX", "jobs").rstrip("/")
DELETE_DYNAMO_ON_FAIL = os.getenv("DELETE_DYNAMO_ON_FAIL")


def now():
    return time()


def get_client(name):
    logger.info("getting %r client", name)
    config = Config(connect_timeout=5, retries={"max_attempts": 0})
    return boto3.client(name, config=config)


class DagRunError(Exception):
    def __init__(self, message, state=None):
        super().__init__(message)
        self.state = state


def js_dump(data, **kw):
    return json.dumps(data, sort_keys=True, separators=(",", ":"), **kw)


def compute_hash(obj, chunk_size=8192, hash_algorithm="sha256"):
    hash_fn = hashlib.new(hash_algorithm)
    while chunk := obj.read(chunk_size):
        hash_fn.update(chunk)
    obj.seek(0)
    return hash_fn.hexdigest()


def exactly_one(**kw):
    keys = [k for k, v in kw.items() if v is not None]
    if len(keys) == 0:
        msg = f"must specify one of: {sorted(kw.keys())}"
        raise ValueError(msg)
    if len(keys) > 1:
        msg = f"must specify only one of: {sorted(kw.keys())} but {keys} are all not None"
        raise ValueError(msg)


@dataclass
class S3Store:
    bucket: str = S3_BUCKET
    prefix: str = S3_PREFIX
    client: "any" = field(default_factory=lambda: boto3.client("s3"))

    def parse_uri(self, name_or_uri):
        p = urlparse(name_or_uri)
        if p.scheme == "s3":
            return p.netloc, p.path[1:]
        return self.bucket, f"{self.prefix}/{name_or_uri}"

    def exists(self, name_or_uri):
        bucket, key = self.parse_uri(name_or_uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response["Error"]["Code"] == "404":
                return False
            raise

    def get(self, name_or_uri):
        bucket, key = self.parse_uri(name_or_uri)
        resp = self.client.get_object(Bucket=bucket, Key=key)
        return resp["Body"].read()

    def put(self, data=None, filepath=None, suffix=None):
        exactly_one(data=data, filepath=filepath)
        # TODO: look for registered serdes through python packaging
        data = open(filepath, "rb") if data is None else BytesIO(data)
        try:
            hash_ = compute_hash(data)
            key = f"{self.prefix}/{hash_}" + (suffix or "")
            self.client.upload_fileobj(data, self.bucket, key)
            return Resource(f"s3://{self.bucket}/{key}")
        finally:
            if filepath is not None:
                data.close()

    def put_js(self, data, **kw):
        return self.put(js_dump(data, **kw).encode(), suffix=".json")

    def get_js(self, uri):
        return json.loads(self.get(uri).decode())

    def writeable(self, fn, suffix=""):
        with NamedTemporaryFile(suffix=suffix) as tmpf:
            fn(tmpf.name)
            tmpf.flush()
            tmpf.seek(0)
            return self.put(filepath=tmpf.name, suffix=suffix)

    def tar(self, dml, path, excludes=()):
        exclude_flags = [["--exclude", x] for x in excludes]
        exclude_flags = [y for x in exclude_flags for y in x]
        with NamedTemporaryFile(suffix=".tar") as tmpf:
            dml(
                "util",
                "tar",
                *exclude_flags,
                str(path),
                tmpf.name,
            )
            return self.put(filepath=tmpf.name, suffix=".tar")

    def untar(self, tar_uri, dest):
        p = urlparse(tar_uri.uri)
        with NamedTemporaryFile(suffix=".tar") as tmpf:
            boto3.client("s3").download_file(p.netloc, p.path[1:], tmpf.name)
            subprocess.run(["tar", "-xvf", tmpf.name, "-C", dest], check=True)


@dataclass
class Dynamo:
    cache_key: str
    run_id: str = field(default_factory=lambda: uuid4().hex)
    timeout: int = field(default=TIMEOUT)
    db: "boto3.client" = field(default_factory=lambda: get_client("dynamodb"))
    tb: str = field(default=os.getenv("DYNAMODB_TABLE"))

    def _update(self, key=None, **kw):
        try:
            return self.db.update_item(
                TableName=self.tb,
                Key={"cache_key": {"S": key or self.cache_key}},
                **kw,
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.info("could not update %r (invalid lock)", self.cache_key)
                return
            raise

    def get(self, key=None):
        logger.info("acquiring lock for %r", self.cache_key)
        ut = now()
        resp = self._update(
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
                ":to": {"N": str(ut - self.timeout)},
            },
            ReturnValues="ALL_NEW",
        )
        if resp is None:
            return
        obj = resp["Attributes"].get("obj", {})
        return obj and json.loads(obj["S"])

    def put(self, obj):
        logger.info("putting data for %r", self.cache_key)
        resp = self._update(
            UpdateExpression="SET #obj = :obj",
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#obj": "obj",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":obj": {"S": json.dumps(obj)},
            },
        )
        return resp is not None

    def unlock(self, key=None):
        logger.info("releasing lock for %r", self.cache_key)
        resp = self._update(
            key,
            UpdateExpression="REMOVE #lk",
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={"#lk": "lock_key"},
            ExpressionAttributeValues={":lk": {"S": self.run_id}},
        )
        return resp is not None

    def delete(self):
        return self.db.delete_item(
            TableName=self.tb,
            Key={"cache_key": {"S": self.cache_key}},
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={"#lk": "lock_key"},
            ExpressionAttributeValues={":lk": {"S": self.run_id}},
        )


@dataclass
class Runner:
    cache_key: str
    kwargs: "any"
    dump: str

    def fmt(self, msg):
        return f"{self.__class__.__name__.lower()} [{self.cache_key}] :: {msg}"

    def run(self):
        dynamo = Dynamo(self.cache_key)
        state = dynamo.get()
        if state is None:
            return {"status": 204, "message": self.fmt("Could not acquire job lock")}
        try:
            logger.info("getting dynamo info")
            state, msg, dump = self.update(state)
            if state is None:
                dynamo.delete()
            else:
                dynamo.put(state) or self.undo(state)
            return {"status": 200, "message": self.fmt(msg), "dump": dump}
        except Exception as e:
            inst = isinstance(e, DagRunError)
            if (inst and e.state is None) or (DELETE_DYNAMO_ON_FAIL and not inst):
                dynamo.delete()
            elif inst:
                dynamo.put(e.state)
            msg = f"Error: {e}\nTraceback:\n----------\n{traceback.format_exc()}"
            return {"status": 400, "message": msg, "dump": None}
        finally:
            dynamo.unlock()

    def undo(self, state):
        pass
