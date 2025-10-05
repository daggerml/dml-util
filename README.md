# dml-util

[![PyPI - Version](https://img.shields.io/pypi/v/dml-util.svg)](https://pypi.org/project/dml-util)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dml-util.svg)](https://pypi.org/project/dml-util)

---

## Overview

`dml-util` provides utilities and adapters for DaggerML, enabling seamless function wrapping, artifact storage, and execution in local and cloud environments. It is designed to work with the [daggerml](https://github.com/daggerml/python-lib) ecosystem.

### Front-End vs Back-End

- **Front-End (User Interface):**
  - The main entrypoints for most users are the `funkify` decorator, `S3Store`, and the included adapters/runners (e.g. conda, hatch, ssh, batch -- all via `funkify`).
  - These let you wrap Python functions as DAG nodes, store artifacts, and run code in a variety of environments with minimal setup.
  - Beginners should start by using these included adapters and runners, as shown in the [examples](examples/).
- **Back-End (Advanced/Extensible):**
  - The back-end consists of the adapters and runners themselves, which handle execution, state, and integration with cloud/local resources.
  - Advanced users can write their own adapters or runners by following the patterns in `src/dml_util/adapters/` and `src/dml_util/runners/`.
  - See the source and docstrings for guidance.

## Installation

```sh
pip install dml-util
```

## Usage

### Wrapping Functions as DAG Nodes

```python
from daggerml import Dml
from dml_util import funkify

@funkify
def add_numbers(dag):
    """Add numbers together.

    Parameters
    ----------
    dag : DmlDag
        The DAG context provided by DaggerML.

    Returns
    -------
    int
        The sum of the input numbers.
    """
    dag.result = sum(dag.argv[1:].value())
    return dag.result

dml = Dml()
with dml.new("simple_addition") as dag:
    dag.add_fn = add_numbers
    dag.sum = dag.add_fn(1, 2, 3)
    print(dag.sum.value())  # Output: 6
```

### S3 Storage

```python
from dml_util import S3Store
s3 = S3Store()
uri = s3.put(b"my data", name="foo.txt").uri
print(uri)  # s3://<bucket>/<prefix>/data/foo.txt
```

### Advanced: Docker, Batch, and ECR

```python
from dml_util import dkr_build, funkify, S3Store

@funkify
def fn(dag):
    *args, denom = dag.argv[1:].value()
    return sum(args) / denom

s3 = S3Store()
tar = s3.tar(dml, ".")
img = dkr_build(tar, ["--platform", "linux/amd64", "-f", "Dockerfile"])
batch_fn = funkify(fn, data={"image": img.value()}, adapter=dag.batch.value())
result = batch_fn(1, 2, 3, 4)
```

### Experimental Api

```python
from dml_util.experimental import api
```

#### Example: A mostly complete working example from the tests

The only modification is that we added a `api.load` call for epistemic purposes.

```python
from contextlib import redirect_stderr, redirect_stdout

from dml_util import S3Store
from tests.util import _root_

def get_tarball(dml):
    s3 = S3Store()
    excludes = [
        "tests/*.py",
        ".pytest_cache",
        ".ruff_cache",
        "**/__about__.py",
        "__pycache__",
        "examples",
        ".venv",
        "**/.venv",
    ]
    with redirect_stdout(None), redirect_stderr(None):
        return s3.tar(dml, str(_root_), excludes=excludes)

class A0(api.Dag):
    dkr_build: Any = funk.dkr_build
    dkr_flags: Any = api.field(default=docker_flags)
    tarball: Node = api.field(default_function=lambda dag: get_tarball(dag.dml))
    not_used0: Any = api.load("path:to:dag::OtherDag")  # loads <...OtherDag>.result
    not_used1: Any = api.load("path:to:dag::OtherDag", "a")  # loads <...OtherDag>.a

    @api.funk(prepop={"x": 1})
    def fn(self, *args):
        """Adds up *args and adds `x` to the result.

        Parameters
        ----------
        *args : ScalarNode
            The values to sum -- should be ints or floats under the hood.

        Other Parameters
        ----------------
        x : ScalarNode, optional
            A value to add to the sum of `*args` (default is 1).
            Keyword-only.

        Returns
        -------
        float
            The sum of the input values plus `x` (defaults to 1).
        """
        return sum([x.value() for x in args]) + self.x.value()

    def dockerify(self, tarball):
        img = self.dkr_build(
            tarball,
            ["--platform", "linux/amd64", "-f", "tests/assets/dkr-context/Dockerfile"],
            timeout=60_000,
            name="img",
        )
        fn = self.wrap(
            self.fn,
            {
                "uri": "docker",
                "data": {"image": img, "flags": self.dkr_flags},
                "adapter": "dml-util-local-adapter",
            },
        )
        return fn

    def run(self, *vals):
        fn = self.dockerify(self.tarball, name="dockerified-fn")
        baz = fn(*vals, name="baz")
        return baz

with A0() as dag:
    vals = [1, 2, 3, 4, 5]
    resp = dag.run(*vals)
    assert resp.value() == sum(vals) + 1
```

Then later, you want to look at the results (dig in and pull node values). You can load this dag (once completed) via:

```python
from daggerml import Dml

dml = Dml()
dag = dml.load("path:to:dag:file::A0")
assert dag.result.load() == sum(vals) + 1
```

#### Example: Basic DAG with fields and steps

Notes:

1. Fields are coerced to nodes, so you must use `.value()` to get their values.
2. Steps can call other steps and the resulting dependency graph is computed automatically (so caching still makes sense).

```python
class MyDag(api.Dag):
    a: int = api.field(default=1)
    b: int = api.field(default=2)

    def step0(self):
        return self.a.value() + self.b.value()

    def step1(self):
        return self.step0(name="s0") * 10

# Instantiate and use the DAG
with MyDag() as dag:
    v = dag.step1(name="my-v")
    print(v.value())  # Output: 30
    dag.commit(v)

from daggerml import Dml
dml = Dml()
dag = dml.load("path:to:dag:file::MyDag")
try:
    dag.result.load().a.value()
except Exception as e:
    print("`a` is not a node in `step1`")
dag.result.load().s0.load().a.value()  # Output: 1
```

#### Example: Using default_function for dynamic fields

```python
from dml_util.experimental import api

class MyDag2(api.Dag):
    a: int = api.field(default=5)
    b: int = api.field(default_function=lambda dag: dag.a.value() * 2)

    def compute(self):
        return self.a.value() + self.b.value()

with MyDag2() as dag:
    dag.commit(dag.compute())
```

#### Example: Loading dag values

```python
from dml_util.experimental import api

class MyDag3(api.Dag):
    a: int = api.field(default=1)
    b: int = api.load("path:to:dag:file::MyDag2", "b")

    def add(self):
        return self.a.value() + self.b.value()
```

#### Example: `api.funk`

This is the experimental equivalent of `dml_util.funkify`.

```python
from dml_util.experimental import api

class MyDag4(api.Dag):
    @api.funk(uri=api.load("batch"), adapter="batch")
    def multiply(self, x, y):
        return x.value() * y.value()
```

## Documentation

- All public functions and classes use [numpy style docstrings](https://numpydoc.readthedocs.io/en/latest/format.html).
- See the [python-lib GitHub repo](https://github.com/daggerml/python-lib) for core DaggerML usage.
- See the [daggerml-cli GitHub repo](https://github.com/daggerml/daggerml-cli) for CLI usage.

## License

`dml-util` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
