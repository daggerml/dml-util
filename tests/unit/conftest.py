"""Common test fixtures for dml-util unit tests."""

import os
import sys
from io import StringIO
from unittest.mock import patch

import pytest

from dml_util.adapters import AdapterBase
from dml_util.core.config import EnvConfig
from tests.util import tmpdir

try:
    from watchtower import CloudWatchLogHandler
except ModuleNotFoundError:
    CloudWatchLogHandler = None


CACHE_PATH = "/tmp/cache"
CACHE_KEY = "test_key"
TEST_RUN_ID = "test-run-id"


@pytest.fixture(autouse=True)
def setup_environment(clear_envvars):
    """Set up test environment variables.

    This fixture sets up common environment variables needed by many tests.
    It also restores the original environment after the test.
    """
    with patch.dict("os.environ"):
        os.environ["DML_CACHE_PATH"] = CACHE_PATH
        os.environ["DML_CACHE_KEY"] = CACHE_KEY
        os.environ["DML_RUN_ID"] = TEST_RUN_ID
        with tmpdir() as tmpd:
            os.environ["DML_FN_CACHE_DIR"] = tmpd
            yield


@pytest.fixture
def test_config():
    """Return a test configuration object."""
    return EnvConfig.from_env(debug=False)


@pytest.fixture
def io_capture():
    """Capture stdout and stderr for testing.

    This fixture redirects stdout and stderr to StringIO objects for capturing
    and testing output. It restores the original stdout and stderr after the test.

    Returns
    -------
    tuple
        (stdout_capture, stderr_capture) as StringIO objects
    """
    original_stdout = sys.stdout
    original_stderr = sys.stderr
    stdout_capture = StringIO()
    stderr_capture = StringIO()
    sys.stdout = stdout_capture
    sys.stderr = stderr_capture
    yield stdout_capture, stderr_capture
    sys.stdout = original_stdout
    sys.stderr = original_stderr


@pytest.fixture
def adapter_setup():
    """Patch the setup method of all adapters."""
    with patch.object(AdapterBase, "_setup", return_value=None) as mock_setup:
        yield mock_setup
