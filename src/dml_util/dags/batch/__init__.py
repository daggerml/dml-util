#!/usr/bin/env python3
import json
import os
import zipfile
from pathlib import Path
from tempfile import NamedTemporaryFile

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


def load():
    s3 = S3()
    with open(_here_ / "cf.json") as f:
        js = json.load(f)
    zipfile = zip_up(s3, _here_ / "src")
    code_data = dict(zip(["S3Bucket", "S3Key"], s3.parse_uri(zipfile.uri)))
    js["Resources"]["Fn"]["Properties"]["Code"] = code_data
    params = {"Bucket": s3.bucket, "Prefix": s3.prefix}
    return js, params, "LambdaFunctionArn", "dml-util-lambda-adapter"
