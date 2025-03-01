import re
from functools import partial
from inspect import getsource
from textwrap import dedent

from daggerml import Resource

from dml_util.executor import SCRIPT_EXEC


def update_query(resource, new_params):
    data = resource.data or {}
    data.update(new_params)
    out = Resource(resource.uri, data=data, adapter=resource.adapter)
    return out


def get_src(f):
    lines = dedent(getsource(f)).split("\n")
    lines = [line for line in lines if not re.match("^@.*funkify", line)]
    return "\n".join(lines)


def _fnk(base_resource, fn_sources, params, fn_name, extra_lines):
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
                return (
                    boto3.client("s3")
                    .get_object(Bucket=p.netloc, Key=p.path[1:])
                    ["Body"].read().decode()
                )
            with open(indata) as f:
                return f.read()

        def _handler(dump):
            outdata = os.environ["DML_OUTPUT_LOC"]
            p = urlparse(outdata)
            if p.scheme == "s3":
                import boto3
                return (
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
        src="\n\n".join(fn_sources),
        fn_name=fn_name,
        eln="\n".join(extra_lines),
    )
    resource = update_query(base_resource, {"script": src, **(params or {})})
    return resource


def funkify(fn=None, base_resource=SCRIPT_EXEC, params=None, extra_fns=(), extra_lines=()):
    if fn is None:
        return partial(
            funkify,
            base_resource=base_resource,
            params=params,
            extra_fns=extra_fns,
            extra_lines=extra_lines,
        )
    fn_sources = [get_src(f) for f in [*extra_fns, fn]]
    resource = _fnk(base_resource, fn_sources, params, fn.__name__, extra_lines)
    object.__setattr__(resource, "fn", fn)
    return resource
