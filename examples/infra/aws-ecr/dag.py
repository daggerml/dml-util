#!/usr/bin/env python3
import json
from pathlib import Path
from time import time

from daggerml import Dml, Resource

from dml_util.common import CFN_EXEC

_here_ = Path(__file__).parent


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Process a name and an optional update flag.")
    parser.add_argument("-n", "--name", type=str, default="ecr")
    args = parser.parse_args()
    with Dml().new(args.name, "creating ecr stack") as dag:
        print(dag._dml("status"))
        with open(_here_ / "cf.json") as f:
            js = json.load(f)
        dag.tpl = js
        dag.cfn_fn = CFN_EXEC
        dag.stack = dag.cfn_fn(
            f"dml-{args.name}",
            dag.tpl,
            {},
            time(),
            sleep=lambda: 5_000,
        )
        dag.result = Resource(dag.stack["Uri"].value())
