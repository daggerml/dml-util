from functools import partial
from inspect import getsource
from textwrap import dedent

from daggerml import Resource

from dml_util.adapter import Adapter


def _fnk(fn, extra_fns, extra_lines):
    def get_src(f):
        lines = dedent(getsource(f)).split("\n")
        while not lines[0].startswith("def "):
            lines.pop(0)
        return "\n".join(lines)

    tpl = dedent(
        """
        #!/usr/bin/env python3
        import logging
        import os
        from urllib.parse import urlparse

        from daggerml import Dml

        logger = logging.getLogger(__name__)

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
            try:
                with Dml() as dml:
                    with dml.new(data=_get_data(), message_handler=_handler) as dag:
                        res = {fn_name}(dag)
                        if dag._ref is None:
                            dag.result = res
            except Exception:
                logger.exception("{fn_name} failed with exception.")
        """
    ).strip()
    src = tpl.format(
        src="\n\n".join([get_src(f) for f in [*extra_fns, fn]]),
        fn_name=fn.__name__,
        eln="\n".join(extra_lines),
    )
    return src


def funkify(
    fn=None,
    uri="script",
    data=None,
    adapter="local",
    extra_fns=(),
    extra_lines=(),
):
    if fn is None:
        return partial(
            funkify,
            uri=uri,
            data=data,
            adapter=adapter,
            extra_fns=extra_fns,
            extra_lines=extra_lines,
        )
    if isinstance(adapter, Resource):
        return Resource(adapter.uri, data={"sub": fn, **(data or {})}, adapter=adapter.adapter)
    adapter_ = Adapter.ADAPTERS.get(adapter)
    if adapter_ is None:
        raise ValueError(f"Adapter: {adapter!r} does not exist")
    data = data or {}
    if isinstance(fn, Resource):
        data = {"sub": fn, **data}
    elif isinstance(fn, str):
        data = {"script": fn, **data}
    else:
        src = _fnk(fn, extra_fns, extra_lines)
        data = {"script": src, **data}
    resource = adapter_.funkify(uri, data=data)
    object.__setattr__(resource, "fn", fn)
    return resource


@funkify
def dkr_build(dag):
    from dml_util.lib.dkr import dkr_build

    tarball = dag.argv[1].value()
    flags = dag.argv[2].value() if len(dag.argv) > 2 else []
    dag.info = dkr_build(tarball.uri, flags)
    dag.result = dag.info["image"]


@funkify
def dkr_push(dag):
    from daggerml import Resource

    from dml_util.lib.dkr import dkr_push

    image = dag.argv[1].value()
    repo = dag.argv[2].value()
    if isinstance(repo, Resource):
        repo = repo.uri
    dag.info = dkr_push(image, repo)
    dag.result = dag.info["image"]
    return


@funkify
def execute_notebook(dag):
    import subprocess
    import sys
    from tempfile import TemporaryDirectory

    from dml_util import S3Store

    def run(*cmd, check=True, **kwargs):
        resp = subprocess.run(cmd, check=False, text=True, capture_output=True, **kwargs)
        if resp.returncode == 0:
            print(resp.stderr, file=sys.stderr)
            return resp.stdout.strip()
        msg = f"STDOUT:\n{resp.stdout}\n\n\nSTDERR:\n{resp.stderr}"
        print(msg)
        if check:
            raise RuntimeError(msg)

    s3 = S3Store()
    with TemporaryDirectory() as tmpd:
        with open(f"{tmpd}/nb.ipynb", "wb") as f:
            f.write(s3.get(dag.argv[1]))
        jupyter = run("which", "jupyter", check=True)
        print(f"jupyter points to: {jupyter}")
        run(
            jupyter,
            "nbconvert",
            "--execute",
            "--to=notebook",
            "--output=foo",
            f"--output-dir={tmpd}",
            f"{tmpd}/nb.ipynb",
        )
        dag.ipynb = s3.put(filepath=f"{tmpd}/foo.ipynb", suffix=".ipynb")
        run(
            jupyter,
            "nbconvert",
            "--to=html",
            f"--output-dir={tmpd}",
            f"{tmpd}/foo.ipynb",
        )
        dag.html = s3.put(filepath=f"{tmpd}/foo.html", suffix=".html")
    dag.result = dag.html
