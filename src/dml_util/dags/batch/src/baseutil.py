import hashlib
import json
import logging
import os
import subprocess
import traceback
from dataclasses import dataclass, field
from io import BytesIO
from pathlib import Path
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
    bucket: str | None = None
    prefix: str | None = None
    client: "any" = field(default_factory=lambda: boto3.client("s3"))

    def __post_init__(self):
        self.bucket = self.bucket or S3_BUCKET
        self.prefix = self.prefix or f"{S3_PREFIX}/data"

    @staticmethod
    def get_write_prefix():
        cache_key = os["DML_CACHE_KEY"]
        return f"{S3_PREFIX}/runs/{cache_key}"

    def parse_uri(self, name_or_uri):
        p = urlparse(name_or_uri)
        if p.scheme == "s3":
            return p.netloc, p.path[1:]
        return self.bucket, f"{self.prefix}/{name_or_uri}"

    def name2uri(self, name):
        bkt, key = self.parse_uri(name)
        return f"s3://{bkt}/{key}"

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

    def put(self, data=None, filepath=None, name=None, suffix=None):
        exactly_one(data=data, filepath=filepath)
        # TODO: look for registered serdes through python packaging
        data = open(filepath, "rb") if data is None else BytesIO(data)
        try:
            if name is None:
                name = compute_hash(data) + (suffix or "")
            key = f"{self.prefix}/{name}"
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


class State:
    pass


@dataclass
class DynamoState(State):
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
        """
        returns:
            None if could not acquire lock
            {} if there's no data
            data otherwise
        """
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
            UpdateExpression="SET #obj = :obj, #ut = :ut",
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#obj": "obj",
                "#ut": "update_time",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":obj": {"S": json.dumps(obj)},
                ":ut": {"N": str(round(now(), 2))},
            },
        )
        return resp is not None

    def unlock(self, key=None):
        logger.info("releasing lock for %r", self.cache_key)
        try:
            resp = self._update(
                key,
                UpdateExpression="REMOVE #lk",
                ConditionExpression="#lk = :lk",
                ExpressionAttributeNames={"#lk": "lock_key"},
                ExpressionAttributeValues={":lk": {"S": self.run_id}},
            )
            return resp is not None
        except Exception:
            pass

    def delete(self):
        return self.db.delete_item(
            TableName=self.tb,
            Key={"cache_key": {"S": self.cache_key}},
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={"#lk": "lock_key"},
            ExpressionAttributeValues={":lk": {"S": self.run_id}},
        )


@dataclass
class LocalState(State):
    cache_key: str
    cache_dir: str = field(init=False)
    state_file: str = field(init=False)

    def __post_init__(self):
        if "DML_FN_CACHE_LOC" in os.environ:
            self.cache_dir = os.environ["DML_FN_CACHE_LOC"]
        elif "DML_FN_CACHE_DIR" in os.environ:
            config_dir = os.environ["DML_FN_CACHE_DIR"]
            self.cache_dir = f"{config_dir}/cache/dml-util/{self.cache_key}"
        else:
            status = subprocess.run(["dml", "status"], check=True, capture_output=True)
            config_dir = json.loads(status.stdout.decode())["config_dir"]
            self.cache_dir = f"{config_dir}/cache/dml-util/{self.cache_key}"
        os.makedirs(self.cache_dir, exist_ok=True)
        self.state_file = Path(self.cache_dir) / "status"

    def put(self, state):
        status_data = {
            "state": state,
            "timestamp": time(),
        }
        with open(self.state_file, "w") as f:
            json.dump(status_data, f)

    def get(self):
        if not self.state_file.exists():
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)["state"]

    def delete(self):
        if os.path.exists(self.state_file):
            os.unlink(self.state_file)

    def unlock(self):
        pass


@dataclass
class Runner:
    cache_key: str
    kwargs: "any"
    dump: str
    retry: bool
    on_error: str = "del"
    state: State = field(init=False)
    state_class = DynamoState

    def __post_init__(self):
        self.state = self.state_class(self.cache_key)

    def _fmt(self, msg):
        return f"{self.__class__.__name__} [{self.cache_key}] :: {msg}"

    def run(self, is_retry=False):
        state = self.state.get()
        if state is None:
            return None, self._fmt("Could not acquire job lock")
        try:
            logger.info("getting info from %r", self.state_class.__name__)
            state, msg, dump = self.update(state)
            if dump and self.retry and not is_retry:
                self.delete(state)
                state, msg, dump = self.update({})
            if state is None:
                self.state.delete()
            else:
                self.state.put(state)
            return dump, self._fmt(msg)
        except Exception as e:
            inst = isinstance(e, DagRunError)
            if (inst and e.state is None) or ((self.on_error == "del") and not inst):
                self.state.delete()
            elif inst:
                self.state.put(e.state)
            msg = f"Error: {e}\nTraceback:\n----------\n{traceback.format_exc()}"
            raise DagRunError(self._fmt(msg)) from e
        finally:
            self.state.unlock()

    def delete(self, state):
        pass


class LambdaRunner(Runner):
    state_class = DynamoState

    def __post_init__(self):
        super().__post_init__()
        self.s3 = S3Store(prefix=f"{S3_PREFIX}/jobs/{self.cache_key}")

    @classmethod
    def handler(cls, event, context):
        try:
            dump, msg = cls(**event).run()
            status = 201 if dump is None else 200
            return {"status": status, "dump": dump, "message": msg}
        except DagRunError as e:
            return {"status": 400, "dump": None, "message": e.message}

    def delete(self, state):
        if self.s3.exists("output.dump"):
            bucket, key = self.s3.parse_uri("output.dump")
            self.s3.client.delete_object(Bucket=bucket, Key=key)
