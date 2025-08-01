#!/usr/bin/env python3
import base64
import re
from dataclasses import dataclass, field
from tempfile import NamedTemporaryFile, TemporaryDirectory
from typing import TYPE_CHECKING
from urllib.parse import urlparse
from uuid import uuid4

from daggerml import Node, Resource

from dml_util.aws import get_client
from dml_util.core.utils import _run_cli

if TYPE_CHECKING:
    import boto3  # noqa: F401


@dataclass
class Ecr:
    ecr: "boto3.client" = field(default_factory=lambda: get_client("ecr"))
    s3: "boto3.client" = field(default_factory=lambda: get_client("s3"))

    @staticmethod
    def _cli(*args, **kwargs):
        return _run_cli(*args, capture_output=False, **kwargs)

    def build(self, tarball, build_flags=(), repo=None):
        p = urlparse(tarball.uri)
        with TemporaryDirectory() as tmpd:
            with NamedTemporaryFile(suffix=".tar") as tmpf:
                self.s3.download_file(p.netloc, p.path[1:], tmpf.name)
                self._cli(["tar", "-xvf", tmpf.name, "-C", tmpd])
            _tag = uuid4().hex
            local_image = f"dml:{_tag}"
            self._cli(
                ["docker", "build", *build_flags, "-t", local_image, tmpd],
            )
        if repo:
            if isinstance(repo, Node):
                repo = repo.value()
            if isinstance(repo, Resource):
                repo = repo.uri
            return self.push(local_image, repo)
        return {"image": Resource(local_image), "tag": _tag}

    def _login(self, proxy_endpoint, password):
        return self._cli(
            [
                "docker",
                "login",
                "--username",
                "AWS",
                "--password-stdin",
                proxy_endpoint,
            ],
            input=password,
        )

    def login(self):
        auth_response = self.ecr.get_authorization_token()
        auth_data = auth_response["authorizationData"][0]
        auth_token = auth_data["authorizationToken"]
        proxy_endpoint = auth_data["proxyEndpoint"]
        decoded_token = base64.b64decode(auth_token).decode("utf-8")
        username, password = decoded_token.split(":")
        return self._login(proxy_endpoint[8:], password)

    def _tag(self, local_image, remote_image):
        self._cli(["docker", "tag", local_image, remote_image])

    def _push(self, remote_image):
        self._cli(["docker", "push", remote_image])

    def push(self, local_image, repo_uri):
        tag = local_image.split(":")[-1]
        remote_image = f"{repo_uri}:{tag}"
        self.login()
        self._tag(local_image, remote_image)
        self._push(remote_image)
        (repo_name,) = re.match(r"^[^/]+/([^:]+)$", repo_uri).groups()
        response = self.ecr.describe_images(repositoryName=repo_name, imageIds=[{"imageTag": tag}])
        digest = response["imageDetails"][0]["imageDigest"]
        return {
            "image": Resource(f"{repo_uri}:{tag}@{digest}"),
            "tag": tag,
            "digest": digest,
        }
