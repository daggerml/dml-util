#!/usr/bin/env python3
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from daggerml import Dml, Resource


def update_uri_query(resource, new_params):
    parsed = urlparse(resource.uri)
    params = parse_qs(parsed.query)
    params.update(new_params)
    query = urlencode(params)
    new_uri = urlunparse(parsed._replace(query=query))
    out = Resource(new_uri, data=resource.data, adapter=resource.adapter)
    return out


if __name__ == "__main__":
    dml = Dml()
    with dml.new("asdf", "qwer") as dag:
        dag.batch = dml.load("batch").result
        with open(Path(__file__).parent / "example_script.py") as f:
            script = f.read()
        dag.fn = update_uri_query(
            dag.batch.value(),
            {"script": script, "image": "python:3.12"},
        )
        dag.sum = dag.fn(1, 2)
        assert dag.sum.value() == 3
        dag.result = dag.sum
        print(f"{dag.sum.value() = }")
