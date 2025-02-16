import os
import re
from functools import partial
from inspect import getsource
from textwrap import dedent
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from daggerml import Resource

BUCKET = os.getenv("DML_S3_BUCKET")
PREFIX = os.getenv("DML_S3_PREFIX")
SCRIPT_EXEC = Resource("dml-util-script-exec", adapter="dml-util-local-adapter")
DOCKER_EXEC = Resource("dml-util-docker-exec", adapter="dml-util-local-adapter")


def exactly_one(**kw):
    keys = [k for k, v in kw.items() if v is not None]
    if len(keys) == 0:
        msg = f"must specify one of: {sorted(kw.keys())}"
        raise ValueError(msg)
    if len(keys) > 1:
        msg = f"must specify only one of: {sorted(kw.keys())} but {keys} are all not None"
        raise ValueError(msg)


def parse_query(resource):
    parsed = urlparse(resource.uri)
    params = parse_qs(parsed.query)
    return params


def update_query(resource, new_params):
    parsed = urlparse(resource.uri)
    params = parse_qs(parsed.query)
    params = {k: v[0] for k, v in params.items()}
    params.update(new_params)
    query = urlencode(params, doseq=True)
    new_uri = urlunparse(parsed._replace(query=query))
    out = Resource(new_uri, data=resource.data, adapter=resource.adapter)
    return out


def get_src(f):
    lines = dedent(getsource(f)).split("\n")
    lines = [line for line in lines if not re.match("^@.*funkify", line)]
    return "\n".join(lines)


def funkify(fn=None, base_resource=SCRIPT_EXEC, params=None, extra_fns=(), extra_lines=()):
    if fn is None:
        return partial(funkify, base_resource=base_resource, params=params, extra_fns=extra_fns)

    tpl = dedent(
        """
        #!/usr/bin/env python3
        import os
        from urllib.parse import urlparse

        from daggerml import Dml

        {src}

        {eln}

        def _get_data():
            indata = os.environ["DML_INPUT_LOC"]
            p = urlparse(indata)
            if p.scheme == "s3":
                import boto3
                data = (
                    boto3.client("s3")
                    .get_object(Bucket=p.netloc, Key=p.path[1:])
                    ["Body"].read().decode()
                )
                return data
            with open(indata) as f:
                return f.read()

        def _handler(dump):
            outdata = os.environ["DML_OUTPUT_LOC"]
            p = urlparse(outdata)
            if p.scheme == "s3":
                import boto3
                data = (
                    boto3.client("s3")
                    .put_object(Bucket=p.netloc, Key=p.path[1:], Body=dump.encode())
                )
            with open(outdata, "w") as f:
                f.write(dump)

        if __name__ == "__main__":
            with Dml(data=_get_data(), message_handler=_handler) as dml:
                with dml.new("test", "test") as dag:
                    res = {fn_name}(dag)
                    if dag._ref is None:
                        dag.result = res
        """
    ).strip()
    src = tpl.format(
        src="\n\n".join([get_src(f) for f in [*extra_fns, fn]]),
        fn_name=fn.__name__,
        eln="\n".join(extra_lines),
    )
    resource = update_query(base_resource, {"script": src, **(params or {})})
    object.__setattr__(resource, "fn", fn)
    return resource
