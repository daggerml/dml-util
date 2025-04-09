#!/usr/bin/env python3
import base64
import json
import re
import subprocess
from tempfile import NamedTemporaryFile, TemporaryDirectory
from urllib.parse import urlparse
from uuid import uuid4

import boto3
import docker
from daggerml import Resource


def _run_cmd(cmd, **kw):
    resp = subprocess.run(cmd, check=False, text=True, stderr=subprocess.PIPE, **kw)
    if resp.returncode != 0:
        msg = f"command: {cmd} failed\nSTDERR:\n-------\n{resp.stderr}"
        raise RuntimeError(msg)


def dkr_build(tarball_uri, build_flags=(), session=None):
    session = session or boto3
    p = urlparse(tarball_uri)
    with TemporaryDirectory() as tmpd:
        with NamedTemporaryFile(suffix=".tar") as tmpf:
            session.client("s3").download_file(p.netloc, p.path[1:], tmpf.name)
            _run_cmd(["tar", "-xvf", tmpf.name, "-C", tmpd])
        _tag = uuid4().hex
        image_tag = f"dml:{_tag}"
        _run_cmd(["docker", "build", *build_flags, "-t", image_tag, tmpd])
    return {"image": Resource(image_tag), "tag": _tag}


def dkr_login(client, dkr_client=None):
    auth_response = client.get_authorization_token()
    auth_data = auth_response["authorizationData"][0]
    auth_token = auth_data["authorizationToken"]
    proxy_endpoint = auth_data["proxyEndpoint"]
    decoded_token = base64.b64decode(auth_token).decode("utf-8")
    username, password = decoded_token.split(":")
    return _run_cmd(
        [
            "docker",
            "login",
            "--username",
            "AWS",
            "--password-stdin",
            proxy_endpoint[8:],
        ],
        input=password,
    )


def dkr_push(local_image, repo_uri):
    client = boto3.client("ecr")
    dkr_client = docker.from_env()
    dkr_login(client, dkr_client=dkr_client)
    tag = local_image.uri.split(":")[-1]
    remote_image = f"{repo_uri}:{tag}"
    img = dkr_client.images.get(local_image.uri)
    img.tag(remote_image)
    for line in dkr_client.api.push(remote_image):
        print(json.dumps(line))
    (repo_name,) = re.match(r"^[^/]+/([^:]+)$", repo_uri).groups()
    response = client.describe_images(repositoryName=repo_name, imageIds=[{"imageTag": tag}])
    digest = response["imageDetails"][0]["imageDigest"]
    return {
        "image": Resource(f"{repo_uri}:{tag}@{digest}"),
        "tag": tag,
        "digest": digest,
    }
