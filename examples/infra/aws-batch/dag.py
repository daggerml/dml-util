#!/usr/bin/env python3
import json
import os
import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile
from time import time

from daggerml import Dml, Resource

from dml_util.common import CFN_EXEC
from dml_util.ingest import S3

_here_ = Path(__file__).parent


def zipit(directory_path, output_zip):
    # FIXME: Use reproducible zip
    with zipfile.ZipFile(output_zip, "w", zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, directory_path)
                zipf.write(file_path, arcname)


def zip_up(s3, path):
    s3 = S3()
    with NamedTemporaryFile(suffix=".zip") as tmpf:
        zipit(path, tmpf.name)
        tmpf.flush()
        return s3.put(filepath=tmpf.name, suffix=".zip")


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Process a name and an optional update flag.")
    parser.add_argument("-n", "--name", type=str, default="batch")
    parser.add_argument("-v", "--verify", action="store_true")
    args = parser.parse_args()
    if args.verify:
        zipit(_here_ / "src", "tmp.zip")
        exit(0)
    s3 = S3()
    with Dml().new(args.name, "creating batch stack") as dag:
        print(dag._dml("status"))
        with open(_here_ / "cf.json") as f:
            js = json.load(f)
        dag.zipfile = zip_up(s3, _here_ / "src")
        code_data = dict(zip(["S3Bucket", "S3Key"], s3.parse_uri(dag.zipfile.value().uri)))
        js["Resources"]["Fn"]["Properties"]["Code"] = code_data
        dag.tpl = js
        dag.cfn_fn = CFN_EXEC
        dag.stack = dag.cfn_fn(
            f"dml-{args.name}",
            dag.tpl,
            {"Bucket": s3.bucket, "Prefix": f"jobs/{args.name}"},
            time(),
            sleep=lambda: 5_000,
        )
        dag.result = Resource(dag.stack["LambdaFunctionArn"].value(), adapter="dml-util-lambda-adapter")
