#!/usr/bin/env python3
import json
import os
import sys
import zipfile
from itertools import cycle
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import sleep

import boto3
from botocore.exceptions import ClientError
from daggerml import Dml, Resource

from dml_util.common import BUCKET, PREFIX, compute_hash

_here_ = Path(__file__).parent
spinner = cycle(["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"])


def zipit(directory_path, output_zip):
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, directory_path)
                zipf.write(file_path, arcname)


def zip_up(path, bucket=BUCKET, prefix=PREFIX):
    client = boto3.client("s3")
    with NamedTemporaryFile(suffix=".zip") as tmpf:
        zipit(path, tmpf.name)
        with open(tmpf.name, "rb") as f:
            hash_ = compute_hash(f)
        key = f"{prefix}/{hash_}.zip"
        client.upload_file(tmpf.name, bucket, key)
    return {"S3Bucket": bucket, "S3Key": key}


def write(msg):
    sys.stdout.write(f"\r{next(spinner)} {msg} ")
    sys.stdout.flush()


def describe_stack(client, name):
    try:
        stack = client.describe_stacks(StackName=name)["Stacks"][0]
    except ClientError as e:
        if "does not exist" in str(e):
            return
        raise
    status = stack["StackStatus"]
    write(f"\r{next(spinner)} Current stack status: {status} ")
    if status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"]:
        return {
            "status": "success",
            "stack_id": stack["StackId"],
            "outputs": {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])},
        }
    elif status in [
        "ROLLBACK_COMPLETE",
        "ROLLBACK_FAILED",
        "CREATE_FAILED",
        "DELETE_FAILED",
    ]:
        events = client.describe_stack_events(StackName=name)["StackEvents"]
        failure_events = [e for e in events if "ResourceStatusReason" in e]
        failure_reasons = [e["ResourceStatusReason"] for e in failure_events]
        return {"status": "failed", "error_reasons": failure_reasons}
    return {"status": "creating"}


def deploy(name, update):
    client = boto3.client("cloudformation")
    response = describe_stack(client, name)
    if response is None or update:
        with open(_here_ / "cf.json") as f:
            js = json.load(f)
        js["Resources"]["Fn"]["Properties"]["Code"] = zip_up(_here_ / "src")
        fn = client.update_stack if update and response is not None else client.create_stack
        try:
            response = fn(
                StackName=name,
                TemplateBody=json.dumps(js),
                Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
            )
        except ClientError as e:
            if e.response["Error"]["Message"].endswith("No updates are to be performed.") and response is not None:
                return response
            raise
        response = {"status": "creating"}
    while response["status"] == "creating":
        try:
            sleep(10)
            response = describe_stack(client, name)
        except ClientError as e:
            print(f"\n⚠️  Error checking stack status: {e}")
            return {"status": "error", "message": str(e)}
    return response


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Process a name and an optional update flag.")
    parser.add_argument("-n", "--name", type=str, default="batch")
    parser.add_argument("-u", "--update", action="store_true")
    args = parser.parse_args()
    with Dml().new(args.name, "creating batch stack") as dag:
        print(dag._dml("status"))
        resp = deploy(f"dml-{args.name}", args.update)
        if resp["status"] == "success":
            dag.stack = Resource(resp["stack_id"])
            dag.lambda_ = Resource(resp["outputs"]["LambdaFunctionArn"], adapter="dml-lambda-adapter")
            dag.result = dag.lambda_
        else:
            print(json.dumps(resp, indent=2))
