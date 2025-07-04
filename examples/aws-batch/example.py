#!/usr/bin/env python3
"""
Example of using Dml's built-in batch executor with a custom function
"""

import os
from pathlib import Path

from daggerml import Dml

import dml_util
from dml_util import S3Store, dkr_build, funkify

_root_ = Path(dml_util.__file__).parent.parent.parent  # repo root
print(f"Using repo root: {_root_}")


@funkify
def fn(dag):
    *args, denom = dag.argv[1:].value()
    dag.result = sum(args) / denom


if __name__ == "__main__":
    dml = Dml()
    s3 = S3Store()
    vals = list(range(4))
    with dml.new("example-batch", __doc__) as dag:
        dag.batch = dml.load("batch").result
        dag.ecr = dml.load("ecr").result

        dag.tar = s3.tar(dml, _root_, excludes=["tests/*.py", "examples/*"])
        dag.bld = dkr_build
        dag.img = dag.bld(
            dag.tar,
            [
                "--platform",
                "linux/amd64",
                "-f",
                "tests/assets/dkr-context/Dockerfile",
            ],
            dag.ecr,
        )
        flags = [
            "--platform",
            "linux/amd64",
            "--add-host=host.docker.internal:host-gateway",
            "-v",
            f"{os.environ['HOME']}/.aws/credentials:/root/.aws/credentials:ro",
            "-e",
            "AWS_SHARED_CREDENTIALS_FILE=/root/.aws/credentials",
            "-e",
            "AWS_DEFAULT_REGION=us-west-2",
            "-e",
            "AWS_REGION=us-west-2",
        ]
        if "AWS_PROFILE" in os.environ:
            flags += ["-e", f"AWS_PROFILE={os.environ['AWS_PROFILE']}"]
        dag.local_fn = funkify(
            fn,
            "docker",
            {"image": dag.img.value(), "flags": flags},
            adapter="local",
        )
        print(f"{dag.local_fn.value() = }")
        local_sum = dag.local_fn(*vals, name="local_sum")
        print("........")
        print(f"{local_sum.value() = }")
        print("........")
        dag.fn = funkify(fn, data={"image": dag.img.value()}, adapter=dag.batch.value())
        print(f"{dag.fn.value() = }")
        dag.sum = dag.fn(*vals)
        assert dag.sum.value() == sum(vals[:-1]) / vals[-1]

        dag.result = dag.sum
        print(f"{dag.sum.value() = }")
