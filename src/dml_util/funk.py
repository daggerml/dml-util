import logging
import os
import re
from contextlib import contextmanager
from functools import partial
from inspect import getsource
from shutil import which
from textwrap import dedent
from typing import TYPE_CHECKING, Any, Callable, Optional, Sequence, Union, overload
from urllib.parse import urlparse

import boto3
from daggerml import Dml, Resource

from dml_util.adapters import AdapterBase

if TYPE_CHECKING:
    import daggerml.core

logger = logging.getLogger(__name__)


def _fnk(fn, extra_fns, extra_lines):
    def get_src(f):
        lines = dedent(getsource(f)).split("\n")
        while not lines[0].startswith("def "):
            lines.pop(0)
        return "\n".join(lines)

    tpl = dedent(
        """
        #!/usr/bin/env python3
        from dml_util import aws_fndag

        {src}

        {eln}

        if __name__ == "__main__":
            with aws_fndag() as dag:
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
    src = re.sub(r"\n{3,}", "\n\n", src)
    return src


@overload
def funkify(
    fn: None = None,
    *,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Resource, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
) -> Callable[[Union[Callable[["daggerml.core.Dag"], Any], Resource]], Resource]: ...


@overload
def funkify(
    fn: Resource,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Resource, str] = "local",
) -> Resource: ...


@overload
def funkify(
    fn: Union[Callable[["daggerml.core.Dag"], Any], str],
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Resource, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
) -> Resource: ...


def funkify(
    fn: Union[None, Callable[["daggerml.core.Dag"], Any], Resource, str] = None,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Resource, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
) -> Union[Resource, Callable[[Callable[["daggerml.core.Dag"], Any]], Resource]]:
    """
    Decorator to funkify a function into a DML Resource.

    Parameters
    ----------
    fn : callable, Resource, optional
        The function to funkify.
    uri : str, optional
        The URI for the resource. Defaults to "script".
    data : dict, optional
        Additional data to include in the resource. Defaults to None.
    adapter : str | Resource, optional
        The adapter to use for the resource. Defaults to "local".
    extra_fns : tuple, optional
        Additional functions to include in the script. Defaults to an empty tuple.
    extra_lines : tuple, optional
        Additional lines to include in the script. Defaults to an empty tuple.

    Returns
    -------
    Resource
        A DML Resource representing the funkified function.

    Notes
    -----
    This is meant to be used as a decorator.

    Examples
    --------
    >>> @funkify
    ... def my_function(dag):
    ...     dag.result = "Hello, DML!"

    They're composable so you can stack them:
    >>> @funkify(uri="docker", data={"image": "my-python:3.8"})
    ... @funkify(uri="hatch", data={"name": "example"})
    ... @funkify
    ... def another_function(dag):
    ...     return "This function runs in the `name` hatch env, inside a docker image."

    And later, if you have a batch cluster set up, you can run it in batch:
    >>> funkify(another_function, data={"image": "my-python:3.8"}, adapter=dag.batch.value())  # doctest: +SKIP
    """
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
        assert isinstance(fn, Resource), "Adapter must be a Resource if fn is a Resource"
        return Resource(adapter.uri, data={"sub": fn, **(data or {})}, adapter=adapter.adapter)
    adapter_ = AdapterBase.ADAPTERS.get(adapter)
    if adapter_ is None:
        adapter_ = which(adapter)
        if adapter_ is None:
            raise ValueError(f"Adapter: {adapter!r} does not exist")
        return Resource(uri=uri, data=data, adapter=adapter_)
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
    from dml_util.lib.dkr import Ecr

    dag.info = Ecr().build(
        dag.argv[1].value(),
        dag.argv[2].value() if len(dag.argv) > 2 else [],
        repo=dag.argv[3].value() if len(dag.argv) > 3 else None,
    )
    dag.result = dag.info["image"]


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


@contextmanager
def aws_fndag():
    def _get_data():
        indata = os.environ["DML_INPUT_LOC"]
        p = urlparse(indata)
        if p.scheme == "s3":
            return boto3.client("s3").get_object(Bucket=p.netloc, Key=p.path[1:])["Body"].read().decode()
        with open(indata) as f:
            return f.read()

    def _handler(dump):
        outdata = os.environ["DML_OUTPUT_LOC"]
        p = urlparse(outdata)
        if p.scheme == "s3":
            return boto3.client("s3").put_object(Bucket=p.netloc, Key=p.path[1:], Body=dump.encode())
        with open(outdata, "w") as f:
            f.write(dump)
            f.flush()

    with Dml.temporary() as dml:
        _data = _get_data()
        try:
            with dml.new(data=_data, message_handler=_handler) as dag:
                dag[".dml/env"] = {k[4:].lower(): v for k, v in os.environ.items() if k.startswith("DML_")}
                yield dag
        except Exception:
            logger.exception("AWS function failed with exception.")
            raise
