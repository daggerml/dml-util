#!/usr/bin/env python3
from dataclasses import dataclass, field
from io import BytesIO
from tempfile import NamedTemporaryFile
from urllib.parse import urlparse

import boto3
from daggerml import Resource

from dml_util.common import BUCKET, PREFIX, compute_hash, exactly_one


@dataclass
class S3:
    bucket: str = BUCKET
    prefix: str = PREFIX
    client: "any" = field(default_factory=lambda: boto3.client("s3"))

    def parse_uri(self, uri):
        p = urlparse(uri)
        if p.scheme == "s3":
            return p.netloc, p.path[1:]
        return self.bucket, f"{self.prefix}/{uri}"

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
            with open(tmpf.name, "rb") as f:
                hash_ = compute_hash(f)
            key = f"{self.prefix}/{hash_}.tar"
            self.client.upload_file(tmpf.name, self.bucket, key)
        return Resource(f"s3://{self.bucket}/{key}")

    def put(self, data=None, filepath=None, suffix=None):
        exactly_one(data=data, filepath=filepath)
        data = open(filepath, "rb") if data is None else BytesIO(data)
        try:
            hash_ = compute_hash(data)
            key = f"{self.prefix}/{hash_}" + (suffix or "")
            self.client.upload_fileobj(data, self.bucket, key)
            return Resource(f"s3://{self.bucket}/{key}")
        finally:
            if filepath is not None:
                data.close()
