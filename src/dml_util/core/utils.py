"""Core utilities with no external dependencies.

This module contains utility functions that are used throughout the DaggerML utilities
package. These functions have minimal or no external dependencies on DaggerML itself,
making them suitable for use in environments where DaggerML is not available.
"""

import errno
import hashlib
import json
import logging
import os
import subprocess
from itertools import islice, product
from time import time

logger = logging.getLogger(__name__)


def tree_map(predicate, fn, item):
    if predicate(item):
        item = fn(item)
    if isinstance(item, list):
        return [tree_map(predicate, fn, x) for x in item]
    if isinstance(item, dict):
        return {k: tree_map(predicate, fn, v) for k, v in item.items()}
    return item


def dict_product(d):
    """
    Given a dictionary of lists, yield all possible combinations of the lists.
    Good for grid searches.

    Parameters
    ----------
    d : dict
        A dictionary where the keys are strings and the values are lists.
        The keys represent the names of the parameters, and the values are the
        possible values for those parameters.

    Yields
    ------
    dict
        A dictionary representing a single combination of parameter values.
        The keys are the same as the input dictionary, and the values are
        the corresponding values from the input lists.

    Examples
    --------
    >>> d = {'a': [1, 2], 'b': ['x', 'y']}
    >>> for combination in dict_product(d):
    ...     print(combination)
    {'a': 1, 'b': 'x'}
    {'a': 1, 'b': 'y'}
    {'a': 2, 'b': 'x'}
    {'a': 2, 'b': 'y'}
    """
    keys = list(d.keys())
    for combination in product(*d.values()):
        yield dict(zip(keys, combination))


def now():
    return time()


def _run_cli(command, capture_output=True, check=True, **kw):
    result = subprocess.run(command, capture_output=capture_output, text=True, check=False, **kw)
    logger.debug("command: %r", command)
    for line in (result.stderr or "").splitlines():
        if line:
            logger.debug("stderr: %r", line)

    logger.debug("end STDERR for command: %r", command)
    if result.returncode != 0:
        msg = f"_run_cli: {command}\n{result.returncode = }"
        if capture_output:
            msg += f"\n{result.stdout}\n\n{result.stderr}"
        if check:
            raise RuntimeError(msg)
        return
    return (result.stdout or "").strip()


def if_read_file(path):
    if os.path.exists(path):
        with open(path) as f:
            return f.read()


def proc_exists(pid):
    try:
        # Check if the process exists
        os.kill(pid, 0)
    except ProcessLookupError:
        return False  # No such process
    except PermissionError:
        return True  # Exists but we don't have permission
    try:
        # Check if it's a zombie process (POSIX only)
        _, status = os.waitpid(pid, os.WNOHANG)
        if status != 0:
            return False  # It's a zombie or has exited
    except ChildProcessError:
        pass  # Not our child process; can't wait on it
    except OSError as e:
        if e.errno != errno.ECHILD:
            raise  # Unexpected error
    return True


def js_dump(data, **kw):
    return json.dumps(data, sort_keys=True, separators=(",", ":"), **kw)


def compute_hash(obj, chunk_size=8192, hash_algorithm="sha256"):
    hash_fn = hashlib.new(hash_algorithm)
    while chunk := obj.read(chunk_size):
        hash_fn.update(chunk)
    obj.seek(0)
    return hash_fn.hexdigest()


def batched(iterable, n, *, strict=False):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError('batched(): incomplete batch')
        yield batch


def exactly_one(**kw):
    keys = [k for k, v in kw.items() if v is not None]
    if len(keys) == 0:
        msg = f"must specify one of: {sorted(kw.keys())}"
        raise ValueError(msg)
    if len(keys) > 1:
        msg = f"must specify only one of: {sorted(kw.keys())} but {keys} are all not None"
        raise ValueError(msg)

