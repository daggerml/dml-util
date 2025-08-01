"""Common test utilities for dml-util tests.

This module contains shared utility functions (not fixtures) for use across the test suite.
"""

from glob import glob
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import NamedTuple

S3_BUCKET = "does-not-exist"
S3_PREFIX = "foopy/barple"
# This file contains only non-pytest utility functions
# All pytest fixtures have been moved to conftest.py

_root_ = Path(__file__).parent.parent


class CliArgs(NamedTuple):
    uri: str
    input: str
    output: str
    error: str
    n_iters: int = 1
    debug: bool = False


def tmpdir():
    return TemporaryDirectory(prefix="dml-util-test-")


def rel_to(x, rel):
    return str(Path(x).relative_to(rel))


def ls_r(path):
    return [rel_to(x, path) for x in glob(f"{path}/**", recursive=True)]
