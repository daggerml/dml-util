#!/usr/bin/env python3
import base64
import re
from tempfile import NamedTemporaryFile, TemporaryDirectory
from urllib.parse import urlparse
from uuid import uuid4

import boto3
from daggerml import Resource

from dml_util.baseutil import _run_cli


def dkr_build(tarball_uri, build_flags=(), repo=None, session=None):
    session = session or boto3
    p = urlparse(tarball_uri)
    with TemporaryDirectory() as tmpd:
        with NamedTemporaryFile(suffix=".tar") as tmpf:
            session.client("s3").download_file(p.netloc, p.path[1:], tmpf.name)
            _run_cli(["tar", "-xvf", tmpf.name, "-C", tmpd], capture_output=False)
        _tag = uuid4().hex
        local_image = f"dml:{_tag}"
        _run_cli(
            ["docker", "build", *build_flags, "-t", local_image, tmpd],
            capture_output=False,
        )
    if repo:
        return dkr_push(local_image, repo, session=session)
    return {"image": Resource(local_image), "tag": _tag}


def dkr_login(client):
    auth_response = client.get_authorization_token()
    auth_data = auth_response["authorizationData"][0]
    auth_token = auth_data["authorizationToken"]
    proxy_endpoint = auth_data["proxyEndpoint"]
    decoded_token = base64.b64decode(auth_token).decode("utf-8")
    username, password = decoded_token.split(":")
    return _run_cli(
        [
            "docker",
            "login",
            "--username",
            "AWS",
            "--password-stdin",
            proxy_endpoint[8:],
        ],
        input=password,
        capture_output=False,
    )


def dkr_push(local_image, repo_uri, session=None):
    client = (session or boto3).client("ecr")
    tag = local_image.uri.split(":")[-1]
    remote_image = f"{repo_uri}:{tag}"
    dkr_login(client)
    _run_cli(["docker", "tag", local_image, remote_image], capture_output=False)
    _run_cli(["docker", "push", remote_image], capture_output=False)
    (repo_name,) = re.match(r"^[^/]+/([^:]+)$", repo_uri).groups()
    response = client.describe_images(repositoryName=repo_name, imageIds=[{"imageTag": tag}])
    digest = response["imageDetails"][0]["imageDigest"]
    return {
        "image": Resource(f"{repo_uri}:{tag}@{digest}"),
        "tag": tag,
        "digest": digest,
    }
