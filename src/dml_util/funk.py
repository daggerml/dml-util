import logging
import os
import re
from contextlib import contextmanager
from functools import partial
from inspect import getsource
from shutil import which
from textwrap import dedent
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Concatenate,
    Dict,
    Literal,
    Optional,
    ParamSpec,
    Sequence,
    Union,
    overload,
)
from urllib.parse import urlparse

import boto3
from daggerml import Dml, Executable

from dml_util.adapters import AdapterBase

if TYPE_CHECKING:
    import daggerml.core


logger = logging.getLogger(__name__)


def _fnk(fn, extra_fns, extra_lines, unpack_args=False):
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
                res = {fn_name}(dag{args})
                if dag.ref is None:
                    dag.commit(res)
        """
    ).strip()
    src = tpl.format(
        src="\n\n".join([get_src(f) for f in [*extra_fns, fn]]),
        fn_name=fn.__name__,
        eln="\n".join(extra_lines),
        args=", *dag.argv[1:]" if unpack_args else "",
    )
    src = re.sub(r"\n{3,}", "\n\n", src)
    return src


_PARAMS = ParamSpec("P")
_DAG_FN = Callable[["daggerml.core.Dag"], Any]
_UNPACKED = Callable[
    Concatenate[Any, _PARAMS], Any
]  # cannot use dml_util.api.Dag here because python typing limitations


@overload
def funkify(
    fn: Executable,
    *,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Executable, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
    prepop: Optional[Dict[str, Any]] = None,
    unpack_args: bool = False,
) -> Executable: ...


@overload
def funkify(
    fn: Union[_DAG_FN, str],
    *,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Executable, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
    prepop: Optional[Dict[str, Any]] = None,
    unpack_args: Literal[False] = False,
) -> Executable: ...


@overload
def funkify(
    fn: None = None,
    *,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Executable, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
    prepop: Optional[Dict[str, Any]] = None,
    unpack_args: Literal[False] = False,
) -> Callable[[Union[_DAG_FN, str]], Executable]: ...


@overload
def funkify(
    fn: Union[_UNPACKED, str],
    *,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Executable, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
    prepop: Optional[Dict[str, Any]] = None,
    unpack_args: Literal[True] = True,
) -> Executable: ...


@overload
def funkify(
    fn: None = None,
    *,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Executable, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
    prepop: Optional[Dict[str, Any]] = None,
    unpack_args: Literal[True] = True,
) -> Callable[[Union[_UNPACKED, str]], Executable]: ...


def funkify(
    fn: Union[None, _DAG_FN, _UNPACKED, Executable, str] = None,
    *,
    uri: str = "script",
    data: Optional[dict] = None,
    adapter: Union[Executable, str] = "local",
    extra_fns: Sequence[Callable] = (),
    extra_lines: Sequence[str] = (),
    prepop: Optional[Dict[str, Any]] = None,
    unpack_args: bool = False,
) -> Union[Executable, Callable[[_DAG_FN], Executable], Callable[[_UNPACKED], Executable]]:
    """
    Decorator to funkify a function into a DML Executable.

    Parameters
    ----------
    fn : callable, Executable, optional
        The function to funkify.
    uri : str, optional
        The URI for the resource. Defaults to "script".
    data : dict, optional
        Additional data to include in the resource. Defaults to None.
    adapter : str | Executable, optional
        The adapter to use for the resource. Defaults to "local".
    extra_fns : tuple, optional
        Additional functions to include in the script. Defaults to an empty tuple.
    extra_lines : tuple, optional
        Additional lines to include in the script. Defaults to an empty tuple.
    prepop : dict, optional
        Prepopulated values for the Executable. Defaults to None.
    unpack_args : bool, optional
        Whether to unpack additional command-line arguments into the function. Defaults to False.

    Returns
    -------
    Executable
        A DML Executable representing the funkified function.

    Notes
    -----
    This is meant to be used as a decorator.

    Examples
    --------
    >>> @funkify
    ... def my_function(dag):
    ...     dag.commit("Hello, DML!")

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
        p = partial(
            funkify,
            uri=uri,
            data=data,
            adapter=adapter,
            extra_fns=extra_fns,
            extra_lines=extra_lines,
            prepop=prepop,
        )
        # note: pyright yells because it's looking for explicit `unpack_args`, so we do this
        if unpack_args:
            return partial(p, unpack_args=True)
        return partial(p, unpack_args=False)
    data = data or {}
    prepop = prepop or {}
    if isinstance(adapter, Executable):
        assert isinstance(fn, Executable), "Adapter must be a Executable if fn is a Executable"
        return Executable(
            adapter.uri,
            data={"sub": fn, **data},
            adapter=adapter.adapter,
            prepop={**fn.prepop, **prepop},
        )
    adapter_ = AdapterBase.ADAPTERS.get(adapter)
    if adapter_ is None:
        adapter_ = which(adapter)
        if adapter_ is None:
            raise ValueError(f"Adapter: {adapter!r} does not exist")
        return Executable(uri=uri, data=data, adapter=adapter_, prepop=prepop)
    if isinstance(fn, Executable):
        data = {"sub": fn, **data}
        prepop = {**fn.prepop, **prepop}
    elif isinstance(fn, str):
        data = {"script": fn, **data}
    else:
        src = _fnk(fn, extra_fns, extra_lines, unpack_args=unpack_args)
        data = {"script": src, **data}
    resource = adapter_.funkify(uri, data=data, prepop=prepop)
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
    dag.commit(dag.info["image"])


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
    dag.commit(dag.html)


@contextmanager
def aws_fndag():
    def _get_data():
        if os.getenv("DML_DEBUG", "").lower() not in ("0", "false", ""):
            logging.basicConfig(level=logging.DEBUG)
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
