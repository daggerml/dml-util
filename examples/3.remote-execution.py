#!/usr/bin/env python3
"""
Example of using Dml's built-in batch executor with a custom function

This example requires a batch cluster already set up to accept dml jobs. You can
create one by running `dml-util-dag batch -i` (adding `-i` will include the
`dml-util` version in the cloudformation stack name so that you can run
different versions simultaneously while we figure out the api).

This dag:
1. Loads the batch and ecr dags
2. Builds a docker image and pushes to ecr
3. Creates a funk that sends jobs to batch when called.
4. Runs a function in batch.

As an extra, we then run that same function in docker locally, and then locally
in python (without docker).
"""

import os
from pathlib import Path

from daggerml import Dml

import dml_util
from dml_util import S3Store, dkr_build, funkify

_root_ = Path(dml_util.__file__).parent.parent.parent  # repo root
print(f"Using repo root: {_root_}")

LOCAL_FLAGS = [
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
    LOCAL_FLAGS += ["-e", f"AWS_PROFILE={os.environ['AWS_PROFILE']}"]


def build_image(dml, dag):
    dag.tar = s3.tar(dml, _root_, excludes=["tests/*.py", "examples/*"])
    dag.bld_fn = dkr_build
    return dag.bld_fn(
        dag.tar,
        [
            "--platform",
            "linux/amd64",
            "-f",
            "tests/assets/dkr-context/Dockerfile",
        ],
        dag.ecr,
        name="image",
    )


@funkify
def fn(dag):
    """A simple function that takes a list of numbers and divides the sum by the last number."""
    *args, denom = dag.argv[1:].value()
    dag.result = sum(args) / denom


if __name__ == "__main__":
    dml = Dml()
    s3 = S3Store()
    vals = list(range(4))
    with dml.new("example-on-batch", __doc__) as dag:
        dag.batch = dml.load("batch").result
        dag.ecr = dml.load("ecr").result

        image = build_image(dml, dag)

        # insert the function into the dag
        dag.batch_fn = funkify(fn, data={"image": dag.image.value()}, adapter=dag.batch.value())
        # call the function and have it run in batch
        dag.batch_sum = dag.batch_fn(*vals)
        # check the result
        assert dag.batch_sum.value() == sum(vals[:-1]) / vals[-1]

        # now let's run it locally to compare...
        dag.local_fn = fn  # note its already been funkified, so we can just assign it
        local_sum = dag.local_fn(*vals, name="local_sum")
        print(f"{local_sum.value() = }")

        # and we can run it locally in that docker image...
        dag.local_in_docker_fn = funkify(
            fn,
            "docker",
            {"image": dag.image.value(), "flags": LOCAL_FLAGS},
            adapter="local",
        )
        local_in_docker_sum = dag.local_in_docker_fn(*vals, name="local_in_docker_sum")
        print("........")
        print(f"{local_in_docker_sum.value() = }")
        print("........")

        dag.result = dag.batch_sum
        print(f"{dag.batch_sum.value() = }")
