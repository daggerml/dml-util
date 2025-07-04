"""Common test utilities for dml-util tests.

This module contains shared utility functions (not fixtures) for use across the test suite.
"""

import os
from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest import TestCase

import boto3

from dml_util.aws.s3 import S3Store

S3_BUCKET = "does-not-exist"
S3_PREFIX = "foopy/barple"
# This file contains only non-pytest utility functions
# All pytest fixtures have been moved to conftest.py

_root_ = Path(__file__).parent.parent
RUN_SLOW_TESTS = bool(os.getenv("DML_RUN_SLOW_TESTS"))


def tmpdir():
    return TemporaryDirectory(prefix="dml-util-test-")


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f"{path}/**", recursive=True)]


class AwsTestCase(TestCase):
    def setUp(self):
        # clear out env variables for safety
        self.old_env = os.environ.copy()
        for k in sorted(os.environ.keys()):
            if k.startswith("AWS_") or k.startswith("DML_"):
                del os.environ[k]
        self.region = "us-east-1"
        # this loads env vars, so import after clearing
        from moto.server import ThreadedMotoServer

        super().setUp()
        self.server = ThreadedMotoServer(port=0)
        self.server.start()
        self.moto_host, self.moto_port = self.server._server.server_address
        self.moto_endpoint = f"http://{self.moto_host}:{self.moto_port}"
        self.aws_env = {
            "AWS_ACCESS_KEY_ID": "foo",
            "AWS_SECRET_ACCESS_KEY": "foo",
            "AWS_REGION": self.region,
            "AWS_DEFAULT_REGION": self.region,
            "AWS_ENDPOINT_URL": self.moto_endpoint,
            "DML_S3_BUCKET": S3_BUCKET,
            "DML_S3_PREFIX": S3_PREFIX,
        }
        for k, v in self.aws_env.items():
            os.environ[k] = v

    def tearDown(self):
        self.server.stop()
        super().tearDown()
        # restore old env
        os.environ.clear()
        os.environ.update(self.old_env)


class FullDmlTestCase(AwsTestCase):
    def setUp(self):
        super().setUp()
        boto3.client("logs", endpoint_url=self.moto_endpoint).create_log_group(logGroupName="dml")
        boto3.client("s3", endpoint_url=self.moto_endpoint).create_bucket(Bucket=S3_BUCKET)
        self.tmpd = tmpdir()
        os.environ["DML_FN_CACHE_DIR"] = self.tmpd.name
        # os.environ["DML_DEBUG"] = "1"

    def tearDown(self):
        s3 = S3Store()
        s3.rm(*s3.ls(recursive=True))
        self.tmpd.cleanup()
        logc = boto3.client("logs", endpoint_url=self.moto_endpoint)
        for log_stream in logc.describe_log_streams(logGroupName="dml")["logStreams"]:
            logc.delete_log_stream(logGroupName="dml", logStreamName=log_stream["logStreamName"])
        logc.delete_log_group(logGroupName="dml")
        super().tearDown()
