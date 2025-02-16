import json
import subprocess
import sys
from shutil import which
from urllib.parse import parse_qs, urlparse

import boto3


def local_():
    prsd = urlparse(sys.argv[1])
    prog = which(prsd.path)
    data = {
        "dump": sys.stdin.read().strip(),
        "cache_key": sys.argv[2],
        "kwargs": parse_qs(prsd.query),
    }
    proc = subprocess.run(
        [prog],
        input=json.dumps(data),
        stdout=subprocess.PIPE,  # stderr passes through to the parent process
        text=True,
    )
    resp = proc.stdout.strip()
    if proc.returncode != 0:
        print(resp, file=sys.stderr)
        sys.exit(1)
    if resp:
        print(resp)


def lambda_():
    parsed_arn = urlparse(sys.argv[1])
    arn = parsed_arn.scheme + ":" + parsed_arn.netloc + parsed_arn.path
    payload = {
        "dump": sys.stdin.read().strip(),
        "cache_key": sys.argv[2],
        "kwargs": parse_qs(parsed_arn.query),
    }
    response = boto3.client("lambda").invoke(
        FunctionName=arn,
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=json.dumps(payload).encode(),
    )
    payload = json.loads(response["Payload"].read())
    if payload.get("message") is not None:
        print(payload["message"], file=sys.stderr)
    if "status" not in payload:  # something went wrong with the lambda
        print(payload, file=sys.stderr)
        sys.exit(1)
    if payload["status"] // 100 in [4, 5]:
        sys.exit(payload["status"])
    if payload.get("dump") is not None:
        print(payload["dump"])
