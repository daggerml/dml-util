import json
import os
import pkgutil
import subprocess
import sys
from importlib import import_module
from pathlib import Path
from time import time

from daggerml import Dml, Resource

from dml_util.common import CFN_EXEC

_here_ = Path(__file__).parent


def imp(name):
    return import_module(f"dml_util.dags.{name}")


def main():
    from argparse import ArgumentParser

    parser = ArgumentParser(description="Spin up a known cfn stack (in a dag).")
    parser.add_argument("name")
    args = parser.parse_args()
    if args.name in ["list"]:
        here = os.path.dirname(__file__)
        for x in os.listdir(here):
            if os.path.isdir(f"{here}/{x}"):
                print(x)
        sys.exit(0)
    with Dml().new(args.name, f"creating {args.name} cfn stack") as dag:
        mod = imp(args.name)
        js, params, output_name, adapter = mod.load()
        dag.tpl = js
        dag.params = params
        dag.adapter = adapter
        dag.output_name = output_name
        dag.cfn_fn = CFN_EXEC
        dag.stack = dag.cfn_fn(
            f"dml-{args.name}",
            dag.tpl,
            params,
            time(),
            sleep=lambda: 5_000,
        )
        dag.result = Resource(dag.stack[output_name].value(), adapter=adapter)
