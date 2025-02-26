import json
import subprocess
import sys
from shutil import which
from time import sleep

from botocore.exceptions import ClientError

from dml_util.baseutil import get_client


def log(*x):
    print(*x, file=sys.stderr)


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
        log(resp)
        sys.exit(1)
    if resp:
        print(resp)


def lambda_():
    try:
        response = get_client("lambda").invoke(
            FunctionName=sys.argv[1],
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=sys.stdin.read().strip().encode(),
        )
    except ClientError as e:
        log(str(e))
        sleep(0.1)
        return
    payload = json.loads(response["Payload"].read())
    if payload.get("message") is not None:
        log(payload["message"])
    if "status" not in payload:  # something went wrong with the lambda
        log(payload)
        sys.exit(1)
    if payload["status"] // 100 in [4, 5]:
        sys.exit(payload["status"])
    if payload.get("dump") is not None:
        print(payload["dump"])
