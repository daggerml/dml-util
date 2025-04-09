import hashlib
import json
import logging
import os
import subprocess
from dataclasses import dataclass, field
from io import BytesIO
from itertools import product
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
    from daggerml.core import Node

    has_dml = True
except ImportError:
    Resource = Node = str
    has_dml = False

logger = logging.getLogger(__name__)
TIMEOUT = 5  # seconds


# Static defaults
_CONFIG_ITEMS = ["s3_bucket", "s3_prefix", "write_loc"]
CONFIG_PATH = ".dml/util-config.json"


def load_config():
    try:
        with open(CONFIG_PATH, "r") as f:
            config = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        config = {}
    config = {k: os.getenv(f"DML_{k.upper()}", config.get(k)) for k in _CONFIG_ITEMS}
    config = {k: v for k, v in config.items() if v is not None}
    return config


def write_config():
    import argparse

    parser = argparse.ArgumentParser(epilog='Example: prog key1=value1 "key2=value=with=equals"')
    parser.add_argument(
        "kvs",
        nargs="+",
        type=str,
        help="Key-value pairs in the format key=value",
    )
    args = parser.parse_args()
    if os.path.exists(CONFIG_PATH):
        with open(CONFIG_PATH) as f:
            config = json.load(f)
    else:
        config = {}
    for kv in args.kvs:
        k, v = kv.split("=", 1)
        config[k] = v
    with open(CONFIG_PATH, "w") as f:
        json.dump(config, f)


CONFIG = load_config()
S3_BUCKET = CONFIG["s3_bucket"]
S3_PREFIX = CONFIG["s3_prefix"]


def tree_map(predicate, fn, item):
    if predicate(item):
        item = fn(item)
    if isinstance(item, list):
        return [tree_map(predicate, fn, x) for x in item]
    if isinstance(item, dict):
        return {k: tree_map(predicate, fn, v) for k, v in item.items()}
    return item


def dict_product(d):
    keys = list(d.keys())
    for combination in product(*d.values()):
        yield dict(zip(keys, combination))


def now():
    return time()


def get_client(name):
    logger.info("getting %r client", name)
    config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "adaptive"})
    return boto3.client(name, config=config)


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
    bucket: str = None
    prefix: str = None
    client: "any" = field(default_factory=lambda: boto3.client("s3"))

    def __post_init__(self):
        self.bucket = self.bucket or S3_BUCKET
        self.prefix = self.prefix or f"{S3_PREFIX}/data"

    @staticmethod
    def get_write_prefix():
        cache_key = os.environ["DML_CACHE_KEY"]
        return f"{S3_PREFIX}/runs/{cache_key}"

    def parse_uri(self, name_or_uri):
        if not isinstance(name_or_uri, str):
            if isinstance(name_or_uri, Node):
                name_or_uri = name_or_uri.value()
            name_or_uri = name_or_uri.uri
        p = urlparse(name_or_uri)
        if p.scheme == "s3":
            return p.netloc, p.path[1:]
        return self.bucket, f"{self.prefix}/{name_or_uri}"

    def name2uri(self, name):
        bkt, key = self.parse_uri(name)
        return f"s3://{bkt}/{key}"

    def _ls(self, recursive=False):
        kw = {}
        if not recursive:
            kw["Delimiter"] = "/"
        prefix = self.prefix.rstrip("/") + "/"
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=prefix, **kw):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                uri = f"s3://{self.bucket}/{key}"
                yield uri

    def ls(self, recursive=False, lazy=False):
        resp = self._ls(recursive=recursive)
        if not lazy:
            resp = list(resp)
        return resp

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

    def put(self, data=None, filepath=None, name=None, uri=None, suffix=None):
        exactly_one(data=data, filepath=filepath)
        exactly_one(name=name, uri=uri, suffix=suffix)
        # TODO: look for registered serdes through python packaging
        data = open(filepath, "rb") if data is None else BytesIO(data)
        try:
            if uri is None:
                if name is None:
                    name = compute_hash(data) + (suffix or "")
                uri = f"s3://{self.bucket}/{self.prefix}/{name}"
            bucket, key = self.parse_uri(uri)
            self.client.upload_fileobj(data, bucket, key)
            return Resource(f"s3://{bucket}/{key}")
        finally:
            if filepath is not None:
                data.close()

    def put_js(self, data, **kw):
        return self.put(js_dump(data, **kw).encode(), suffix=".json")

    def get_js(self, uri):
        return json.loads(self.get(uri).decode())

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

    def rm(self, *name_or_uris):
        if len(name_or_uris) == 0:
            return
        buckets, keys = zip(*[self.parse_uri(x) for x in name_or_uris])
        self.client.delete_objects(
            Bucket=buckets[0],
            Delete={"Objects": [{"Key": x} for x in keys]},
        )


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
        try:
            return self.db.delete_item(
                TableName=self.tb,
                Key={"cache_key": {"S": self.cache_key}},
                ConditionExpression="#lk = :lk",
                ExpressionAttributeNames={"#lk": "lock_key"},
                ExpressionAttributeValues={":lk": {"S": self.run_id}},
            )
        except ClientError as e:
            if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
                raise


@dataclass
class LocalState(State):
    cache_key: str
    state_file: str = field(init=False)

    def __post_init__(self):
        if "DML_FN_CACHE_DIR" in os.environ:
            cache_dir = os.environ["DML_FN_CACHE_DIR"]
        else:
            from dml_util import __version__

            status = subprocess.run(["dml", "status"], check=True, capture_output=True)
            config_dir = json.loads(status.stdout.decode())["config_dir"]
            cache_dir = f"{config_dir}/cache/dml-util/v{__version__}"
        os.makedirs(cache_dir, exist_ok=True)
        self.state_file = Path(cache_dir) / f"{self.cache_key}.json"

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
    retry: bool  # TODO: executor caching
    state: State = field(init=False)
    state_class = LocalState

    def __post_init__(self):
        meta_kwargs = (self.kwargs or {}).get("dml_meta", {})
        bucket = meta_kwargs.get("s3_bucket", S3_BUCKET)
        prefix = meta_kwargs.get("s3_prefix", S3_PREFIX)
        clsname = type(self).__name__
        self.state = self.state_class(self.cache_key)
        self.prefix = f"{prefix}/exec/{clsname.lower()}"
        self.env = {
            "DML_S3_BUCKET": bucket,
            "DML_S3_PREFIX": prefix,
            "DML_CACHE_KEY": self.cache_key,
            "DML_WRITE_LOC": f"s3://{bucket}/{self.prefix}/data/{self.cache_key}",
        }

    def _to_data(self):
        sub = self.kwargs["sub"]
        ks = {
            "cache_key": self.cache_key,
            "kwargs": sub["data"],
            "dump": self.dump,
            "retry": self.retry,
        }
        return sub["uri"], json.dumps(ks), sub["adapter"]

    def _fmt(self, msg):
        return f"{self.__class__.__name__} [{self.cache_key}] :: {msg}"

    def put_state(self, state):
        self.state.put(state)

    def run(self):
        state = self.state.get()
        delete = False
        if state is None:
            return {}, self._fmt("Could not acquire job lock")
        if "result" in state:
            return state["result"], self._fmt("returning cached...")
        try:
            logger.info("getting info from %r", self.state_class.__name__)
            state, msg, response = self.update(state)
            if state is None:
                delete = True
            else:
                if response.get("dump") is not None:
                    state["result"] = response
                    self.gc(state)
                self.put_state(state)
            return response, self._fmt(msg)
        except Exception:
            delete = True
            raise
        finally:
            if delete:
                if not os.getenv("DML_NO_GC"):
                    self.gc(state)
                self.state.delete()
            else:
                self.state.unlock()

    def gc(self, state):
        pass


class LambdaRunner(Runner):
    state_class = DynamoState

    def __post_init__(self):
        super().__post_init__()
        self.s3 = S3Store(prefix=f"{self.prefix}/jobs/{self.cache_key}")

    @property
    def output_loc(self):
        return self.s3.name2uri("output.dump")

    @classmethod
    def handler(cls, event, context):
        try:
            response, msg = cls(**event).run()
            status = 200 if response else 201
            return {"status": status, "response": response, "message": msg}
        except Exception as e:
            return {"status": 400, "response": {}, "message": str(e)}

    def gc(self, state):
        if self.s3.exists(self.output_loc):
            self.s3.rm(self.output_loc)
