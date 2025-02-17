#!/usr/bin/env python3
from pathlib import Path

from daggerml import Dml

from dml_util.common import update_query

if __name__ == "__main__":
    dml = Dml()
    vals = [1, 2, 3]
    with dml.new("asdf", "qwer") as dag:
        dag.batch = dml.load("batch").result
        with open(Path(__file__).parent / "example_script.py") as f:
            script = f.read()
        dag.fn = update_query(
            dag.batch.value(),
            {"script": script, "image": "python:3.12"},
        )
        dag.sum = dag.fn(*vals)
        assert dag.sum.value() == sum(vals)
        dag.result = dag.sum
        print(f"{dag.sum.value() = }")
