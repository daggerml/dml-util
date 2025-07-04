"""Local adapter for running DaggerML functions.

This module provides a local adapter for running DaggerML functions.
The local adapter allows running DaggerML functions in the local environment
using various runners, such as script runners, conda runners, etc.

Classes
-------
LocalAdapter
    Local adapter for running DaggerML functions.
"""

import json
from dataclasses import dataclass

from dml_util.adapters.base import AdapterBase
from dml_util.core.config import EnvConfig, InputConfig


@dataclass
class LocalAdapter(AdapterBase):
    """Local adapter for running DaggerML functions."""

    ADAPTER = "dml-util-local-adapter"

    @classmethod
    def resolve(cls, uri: str) -> str:
        """Resolve a URI to a local runner."""
        from dml_util.runners.base import RunnerBase

        return RunnerBase._RUNNERS[uri]

    @classmethod
    def funkify(cls, uri, data):
        """Create a Resource from a URI and data."""
        data = cls.resolve(uri).funkify(**data)
        if isinstance(data, tuple):
            uri, data = data
        return super().funkify(uri, data)

    @classmethod
    def send_to_remote(cls, uri, config: EnvConfig, dump: str) -> tuple[str, str]:
        """Send data to a local runner.

        Parameters
        ----------
        uri : str
            The runner URI.
        config : EnvConfig
            Configuration for the run.
        dump : str
            The payload to send.

        Returns
        -------
        tuple[str, str]
            A tuple of (response, message).
        """
        runner = cls.resolve(uri)(config, InputConfig(**json.loads(dump)))
        return runner.run()
