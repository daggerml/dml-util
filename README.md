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
    dag.result = sum(args) / denom

s3 = S3Store()
tar = s3.tar(dml, ".")
img = dkr_build(tar, ["--platform", "linux/amd64", "-f", "Dockerfile"])
batch_fn = funkify(fn, data={"image": img.value()}, adapter=dag.batch.value())
result = batch_fn(1, 2, 3, 4)
```

## Documentation

- All public functions and classes use [numpy style docstrings](https://numpydoc.readthedocs.io/en/latest/format.html).
- See the [python-lib GitHub repo](https://github.com/daggerml/python-lib) for core DaggerML usage.
- See the [daggerml-cli GitHub repo](https://github.com/daggerml/daggerml-cli) for CLI usage.

## License

`dml-util` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.
