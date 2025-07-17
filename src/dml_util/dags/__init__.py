import json
import os
import pkgutil
import subprocess
import sys
from argparse import ArgumentParser
from importlib import import_module
from pathlib import Path
from time import time

from daggerml import Dml, Resource

from dml_util import __version__

_here_ = Path(__file__).parent


def imp(name):
    return import_module(f"dml_util.dags.{name}")


def main():
    parser = ArgumentParser(description="Spin up a known cfn stack (in a dag).")
    parser.add_argument("name", nargs="?")
    parser.add_argument("-f", "--filepath")
    parser.add_argument("-i", "--add-version-info", action="store_true")
    parser.add_argument("-l", "--list", action="store_true")
    args = parser.parse_args()
    if args.list:
        here = os.path.dirname(__file__)
        for x in os.listdir(here):
            if os.path.isdir(f"{here}/{x}") and x != "__pycache__":
                print(x)
        sys.exit(0)
    assert args.name is not None, "name is required unless --list is set"
    with Dml().new(args.name, f"creating {args.name} cfn stack") as dag:
        if args.filepath is None:
            mod = imp(args.name)
            tpl, params, output_name, adapter, version = mod.load()
        else:
            resp = subprocess.run([args.filepath], check=True, capture_output=True, text=True)
            tpl, params, output_name, adapter, version = json.loads(resp.stdout)
        dag.tpl = tpl
        dag.params = params
        dag.adapter = adapter
        dag.output_name = output_name
        dag.cfn_fn = Resource("cfn", adapter="dml-util-local-adapter")
        name = f"dml-{args.name}"
        if args.add_version_info:
            name = f"{name}-v{version}"
        dag.stack = dag.cfn_fn(
            name,
            dag.tpl,
            params,
            time(),
            sleep=lambda: 5_000,
        )
        dag.result = Resource(dag.stack[output_name].value(), adapter=adapter)
