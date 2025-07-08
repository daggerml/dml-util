"""DaggerML-specific utilities.

A minimal replacement for `daggerml` for environments where it is not available.
"""

from dataclasses import dataclass, field
from traceback import format_exception
from typing import Optional, Union

# Check if daggerml is available
try:
    from daggerml import Dml, Error, Resource
    from daggerml.core import Node

    has_daggerml = True
except ModuleNotFoundError:
    has_daggerml = False
    Dml = None

    @dataclass
    class Error(Exception):  # noqa: F811
        """
        Custom error type for DaggerML.

        Parameters
        ----------
        message : Union[str, Exception]
            Error message or exception
        context : dict, optional
            Additional error context
        code : str, optional
            Error code
        """

        message: Union[str, Exception]
        context: dict = field(default_factory=dict)
        code: Optional[str] = None

        def __post_init__(self):
            if isinstance(self.message, Error):
                ex = self.message
                self.message = ex.message
                self.context = ex.context
                self.code = ex.code
            elif isinstance(self.message, Exception):
                ex = self.message
                self.message = str(ex)
                self.context = {"trace": format_exception(type(ex), value=ex, tb=ex.__traceback__)}
                self.code = type(ex).__name__
            else:
                self.code = type(self).__name__ if self.code is None else self.code

        def __str__(self):
            return "".join(self.context.get("trace", [self.message]))

    @dataclass
    class Resource:
        """Placeholder for Resource class if DaggerML is not available."""

        uri: str
        data: Optional[dict] = None
        adapter: Optional[str] = None

    @dataclass
    class Node:
        """Placeholder for Node class if DaggerML is not available."""

        _value: Union[str, "Resource"]

        def value(self):
            return self._value
