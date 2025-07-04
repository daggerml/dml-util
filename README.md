# dml-util

[![PyPI - Version](https://img.shields.io/pypi/v/dml-util.svg)](https://pypi.org/project/dml-util)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/dml-util.svg)](https://pypi.org/project/dml-util)

---

## Table of Contents

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install dml-util
```

## License

`dml-util` is distributed under the terms of the [MIT](https://spdx.org/licenses/MIT.html) license.

### TODO List:

#### Cli Changes

- [ ] Add currently running jobs to cache db.
  - Should include:
    - executor
    - status
    - start time
    - last-update time
    - a list of all the sub-jobs (by cache-key)
  - This will allow us to cancel jobs via the cli
  - This info should be conveyed via stderr by the adapters

#### Python-lib Changes

#### Util Changes

- [ ] update app to use the new logging format
  - Lazy load logs from cloudwatch
- [ ] Add caching functionality to the UI
- [ ] Custom dashboards
- [ ] git integration
- [ ] cancel running jobs via executor
- [ ] Add docstrings to each module specifying the protocol
- [ ] Use docstrings to generate tests

- add neovim command to test only not slow. And another to test All.
