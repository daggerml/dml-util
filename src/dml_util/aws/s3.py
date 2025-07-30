"""S3 storage utilities."""

import json
import logging
import os
import subprocess
from dataclasses import dataclass, field, replace
from io import BytesIO
from itertools import groupby
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Sequence, Union, overload
from urllib.parse import urlparse

import boto3
from daggerml import Node, Resource

from dml_util.aws import get_client
from dml_util.core.utils import batched, compute_hash, exactly_one, js_dump

logger = logging.getLogger(__name__)


@dataclass
class S3Store:
    """
    S3 Store for DML

    Parameters
    ----------
    bucket : str
        S3 bucket name. Defaults to the value of the environment variable "DML_S3_BUCKET".
    prefix : str
        S3 prefix. Defaults to the value of the environment variable "DML_S3_PREFIX".
    client : boto3.client, optional
        Boto3 S3 client. Defaults to a new client created using the `get_client` function.

    Notes
    -----
    - If `prefix` is not provided, "/data" is appended to the `DML_S3_PREFIX` environment variable.
    - `prefix` is stripped of leading and trailing slashes, so if you want to use a prefix like "/foo/", you'll need to
      handle those uris directly. E.g. to put data at "s3://my-bucket//foo/bar", you would use
      `S3Store().put(data, uri="s3://my-bucket//foo/bar")`.

    Examples
    --------
    >>> s3 = S3Store(bucket="my-bucket", prefix="my-prefix")
    >>> s3.put(data=b"Hello, World!", name="greeting.txt")  # doctest: +SKIP
    Resource(uri='s3://my-bucket/my-prefix/greeting.txt')
    >>> s3.ls(recursive=True)  # doctest: +SKIP
    ['s3://my-bucket/my-prefix/greeting.txt']
    >>> s3.get("greeting.txt")  # doctest: +SKIP
    b'Hello, World!'
    >>> s3.exists("greeting.txt")  # doctest: +SKIP
    True
    >>> s3.rm("greeting.txt")  # doctest: +SKIP
    >>> s3.exists("greeting.txt")  # doctest: +SKIP
    False
    >>> s3.put_js({"key": "value"}, name="data")  # doctest: +SKIP
    Resource(uri='s3://my-bucket/my-prefix/data.json')
    >>> s3.get_js("data")  # doctest: +SKIP
    {'key': 'value'}
    >>> s3.tar(dml, path="my_data", excludes=["*.tmp"])  # doctest: +SKIP
    Resource(uri='s3://my-bucket/my-prefix/my_data.tar')
    >>> s3.untar("s3://my-bucket/my-prefix/my_data.tar", dest="my_data")  # doctest: +SKIP
    # Extracts the tar archive to the local directory "my_data"
    >>> s3.cd("new-prefix")
    S3Store(bucket='my-bucket', prefix='my-prefix/new-prefix')
    >>> s3.cd("..")  # Go back to the previous prefix
    S3Store(bucket='my-bucket', prefix='')
    """

    bucket: str = field(default_factory=lambda: os.getenv("DML_S3_BUCKET"))
    prefix: str = None
    client: "boto3.client" = field(default_factory=lambda: get_client("s3"), repr=False)

    def __post_init__(self):
        if self.prefix is None:
            self.prefix = os.getenv("DML_S3_PREFIX", "") + "/data"
        self.prefix = self.prefix.strip("/")
        logger.debug("Initialized S3Store at s3://%s/%s", self.bucket, self.prefix)

    def parse_uri(self, name_or_uri):
        """
        Parse a URI or name into bucket and key.

        Examples
        --------
        >>> s3 = S3Store(bucket="my-bucket", prefix="my-prefix")
        >>> s3.parse_uri("s3://my-other-bucket/my-key")
        ('my-other-bucket', 'my-key')
        >>> s3.parse_uri("my-key")
        ('my-bucket', 'my-prefix/my-key')
        >>> s3.parse_uri(Resource("s3://my-other-bucket/my-key"))
        ('my-other-bucket', 'my-key')
        """
        if isinstance(name_or_uri, Node):
            name_or_uri = name_or_uri.value()
        if isinstance(name_or_uri, Resource):
            name_or_uri = name_or_uri.uri
        p = urlparse(name_or_uri)
        if p.scheme == "s3":
            return p.netloc, p.path[1:]
        key = f"{self.prefix}/{name_or_uri}" if self.prefix else name_or_uri
        return self.bucket, key

    def _name2uri(self, name):
        bkt, key = self.parse_uri(name)
        return f"s3://{bkt}/{key}"

    def _ls(self, uri=None, recursive=False):
        kw = {}
        if not recursive:
            kw["Delimiter"] = "/"
        bucket, prefix = self.parse_uri(uri or f"s3://{self.bucket}/{self.prefix}")
        prefix = prefix.rstrip("/") + "/" if prefix else ""
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix, **kw):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                uri = f"s3://{bucket}/{key}"
                yield uri

    def ls(self, s3_root=None, *, recursive=False, lazy=False):
        """
        List objects in the S3 bucket.

        Parameters
        ----------
        s3_root : str, optional
            Name or s3 root to list. Defaults to s3://<bucket>/<prefix>/.
        recursive : bool
            If True, list all objects recursively. Defaults to False.
        lazy : bool
            If True, return a generator. Defaults to False.

        Returns
        -------
        generator or list
            A generator or list of S3 URIs.

        """
        resp = self._ls(s3_root, recursive=recursive)
        if not lazy:
            resp = list(resp)
        return resp

    def exists(self, name_or_uri):
        bucket, key = self.parse_uri(name_or_uri)
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as e:
            if getattr(e, "response", {}).get("Error", {}).get("Code") == "404":
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
            if uri is None and name is None:
                name = compute_hash(data) + (suffix or "")
            bucket, key = self.parse_uri(uri or name)
            self.client.upload_fileobj(data, bucket, key)
            return Resource(f"s3://{bucket}/{key}")
        finally:
            if filepath is not None:
                data.close()

    def put_js(self, data, uri=None, **kw) -> Resource:
        suffix = ".json" if uri is None else None
        return self.put(js_dump(data, **kw).encode(), uri=uri, suffix=suffix)

    def get_js(self, uri):
        return json.loads(self.get(uri).decode())

    def tar(self, dml, path, excludes=()):
        """Create a tar archive and store it in S3."""
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
        """Extract a tar archive from S3 to a local directory."""
        p = urlparse(tar_uri.uri)
        with NamedTemporaryFile(suffix=".tar") as tmpf:
            self.client.download_file(p.netloc, p.path[1:], tmpf.name)
            subprocess.run(["tar", "-xvf", tmpf.name, "-C", dest], check=True)

    @overload
    def rm(self, name_or_uri: Sequence[Union[str, Resource, Node]]) -> None:
        """Remove objects from S3."""
        ...

    @overload
    def rm(self, *name_or_uri: Union[str, Resource, Node]) -> None:
        """Remove objects from S3."""
        ...

    def rm(self, *name_or_uris: Union[str, Resource, Node, Sequence[Union[str, Resource, Node]]]) -> None:
        """Remove objects from S3."""
        if len(name_or_uris) == 1 and isinstance(name_or_uris[0], (list, tuple)):
            name_or_uris = tuple(name_or_uris[0])
        assert not any(isinstance(x, (tuple, list)) for x in name_or_uris), "rm does not support nested lists or tuples"
        if len(name_or_uris) == 0:
            return
        for bucket, objs in groupby(map(self.parse_uri, sorted(name_or_uris)), key=lambda x: x[0]):
            for batch in batched((x[1] for x in objs), 1000):
                self.client.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": [{"Key": x} for x in batch]},
                )

    def cd(self, new_prefix) -> "S3Store":
        """Change the prefix of the S3 store."""
        root = Path(self.prefix)
        new_path = str((root / new_prefix).resolve().relative_to(os.getcwd()))
        return replace(self, prefix=new_path if new_path != "." else "")
