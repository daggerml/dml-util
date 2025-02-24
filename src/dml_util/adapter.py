import json
import subprocess
import sys
from shutil import which

import boto3


def local_():
    prog = which(sys.argv[1])
    proc = subprocess.run(
        [prog],
        input=sys.stdin.read(),
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
    response = boto3.client("lambda").invoke(
        FunctionName=sys.argv[1],
        InvocationType="RequestResponse",
        LogType="Tail",
        Payload=sys.stdin.read().strip().encode(),
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
