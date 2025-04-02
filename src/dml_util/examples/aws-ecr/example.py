#!/usr/bin/env python3
from pathlib import Path

from daggerml import Dml

from dml_util import S3Store, dkr_build, dkr_push, funkify

_here_ = Path(__file__).parent.parent.parent.parent.parent

print(f"{_here_ = }")


@funkify
def fn(dag):
    *args, denom = dag.argv[1:].value()
    dag.result = sum(args) / denom


if __name__ == "__main__":
    dml = Dml()
    s3 = S3Store()
    vals = list(range(4))
    with dml.new("asdf", "qwer") as dag:
        dag.batch = dml.load("batch").result
        dag.ecr = dml.load("ecr").result

        dag.tar = s3.tar(dml, _here_, excludes=["tests/*.py"])
        dag.bld = dkr_build
        dag.img = dag.bld(
            dag.tar,
            [
                "--platform",
                "linux/amd64",
                "-f",
                "tests/assets/dkr-context/Dockerfile",
            ],
        )
        dag.push = dkr_push
        dag.remote_image = dag.push(dag.img, dag.ecr)
        dag.fn = funkify(fn, data={"image": dag.remote_image.value()}, adapter=dag.batch.value())
        print(f"{dag.fn.value() = }")
        dag.sum = dag.fn(*vals)
        assert dag.sum.value() == sum(vals[:-1]) / vals[-1]

        dag.result = dag.sum
        print(f"{dag.sum.value() = }")
