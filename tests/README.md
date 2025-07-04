# Tests

The tests directory consists of the following structure:

```
tests/
├── __init__.py
├── util.py
├── conftest.py
├── assets/
├── integration/
└── unit/
```

The `tests/unit/` structure follows the `src/` structure, with each module
having its own test file. For example, the `src/dml_util/adapters/base.py`
module has a corresponding `tests/unit/adapters/test_base.py` file.

The `tests/assets/` directory contains dml function implementations that are
useful for testing.

The `tests/util.py` file contains utility functions and base classes that are
used across tests. The base classes set up things like our moto server and
ensure environment variables are handled correctly.

The `tests/integration/` directory contains integration tests that cover the
full use-case based application of the library. It checks to ensure you can
call one function from another without anything getting messed up between.

The unit tests should be runnable even without daggerml installed.
