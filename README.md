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
    print(dag.step1().value())  # Output: 30

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
