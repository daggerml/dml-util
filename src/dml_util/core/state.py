"""State management for DaggerML runners.

This module provides classes for managing state across different runners. The state
management system allows runners to store and retrieve state information, which is
particularly useful for long-running or distributed tasks.
"""

import json
import os
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from time import time

TIMEOUT = 5  # seconds


class State:
    """Base class for state management.

    This is an abstract base class that defines the interface for state management.
    Concrete implementations should provide methods for storing, retrieving, and
    deleting state information.

    Methods
    -------
    put(state)
        Store state information.
    get()
        Retrieve state information.
    delete()
        Delete state information.
    unlock()
        Release any locks on the state.
    """

    def put(self, state):
        """Store state information."""
        raise NotImplementedError("Subclasses must implement this method.")

    def get(self):
        """Retrieve state information."""
        raise NotImplementedError("Subclasses must implement this method.")

    def delete(self):
        """Delete state information."""
        raise NotImplementedError("Subclasses must implement this method.")

    def unlock(self):
        """Release any locks on the state."""
        raise NotImplementedError("Subclasses must implement this method.")


@dataclass
class LocalState(State):
    """Local filesystem-based state management.

    This class implements state management using the local filesystem.
    State is stored as JSON files in a cache directory.

    Parameters
    ----------
    cache_key : str
        Unique identifier for the state file.

    Attributes
    ----------
    state_file : Path
        Path to the JSON file where state is stored.
    """

    cache_key: str
    state_file: str = field(init=False)

    def __post_init__(self):
        if "DML_FN_CACHE_DIR" in os.environ:
            cache_dir = os.environ["DML_FN_CACHE_DIR"]
        else:
            from dml_util import __version__

            status = subprocess.run(["dml", "status"], check=True, capture_output=True)
            config_dir = json.loads(status.stdout.decode())["config_dir"]
            cache_dir = f"{config_dir}/cache/dml-util/v{__version__}"
        os.makedirs(cache_dir, exist_ok=True)
        self.state_file = Path(cache_dir) / f"{self.cache_key}.json"

    def put(self, state):
        """Write state to file."""
        status_data = {
            "state": state,
            "timestamp": time(),
        }
        with open(self.state_file, "w") as f:
            json.dump(status_data, f)

    def get(self):
        """Read state from file."""
        if not self.state_file.exists():
            return {}
        with open(self.state_file, "r") as f:
            return json.load(f)["state"]

    def delete(self):
        """Delete state file."""
        if os.path.exists(self.state_file):
            os.unlink(self.state_file)

    def unlock(self):
        """No-op for local state."""
        pass
