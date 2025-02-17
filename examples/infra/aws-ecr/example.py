#!/usr/bin/env python3
from pathlib import Path

from daggerml import Dml

from dml_util import S3, dkr_build, dkr_push, funkify, query_update

_here_ = Path(__file__).parent


def fn(dag):
    dag.result = sum(dag.argv[1:].value())


if __name__ == "__main__":
    dml = Dml()
    s3 = S3()
    vals = list(range(4))
    with dml.new("asdf", "qwer") as dag:
        dag.batch = dml.load("batch").result
        dag.ecr = dml.load("ecr").result

        dag.tar = s3.tar(dml, _here_ / "src")
        dag.dkr = dkr_build
        dag.img = dag.dkr(
            dag.tar,
            ["--platform", "linux/amd64"],
        )
        dag.push = dkr_push
        dag.remote_image = dag.push(dag.img, dag.ecr)
        dag.chg = query_update
        dag.fn = funkify(fn, dag.batch.value(), params={"image": dag.remote_image.value().uri})
        dag.sum = dag.fn(*vals)
        assert dag.sum.value() == sum(vals)

        dag.result = dag.sum
        print(f"{dag.sum.value() = }")
