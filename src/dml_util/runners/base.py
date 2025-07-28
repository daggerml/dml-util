"""Runner base class for executing code in different environments.

This module defines the base RunnerBase class for executing code in different
environments. Runners are used to execute tasks in various environments, such as
local, container, or remote environments. They provide a consistent interface
for task execution with state management and logging.
"""

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Union
from warnings import warn

from dml_util.core.config import EnvConfig, InputConfig
from dml_util.core.state import LocalState, State

logger = logging.getLogger(__name__)

runners = {}


@dataclass
class RunnerBase:
    """Base Runner class for executing code in different environments.

    This class provides a framework for running tasks with state management and logging.
    Subclasses must implement specific methods and adhere to the defined interface.

    Notes
    -----
    Subclasses must implement one or more of the following methods:
    - `run`: Executes the primary task logic (e.g., `WrappedRunner`, `SshRunner`).
    - `update`: Updates the state and handles task execution (e.g., `ScriptRunner`).

    The difference being tha tthe `run` method will handle all of the locking and state
    management for you, so if you override it, you should not call `self.put_state` or
    `self.state.get` directly.

    Examples
    --------
    >>> class MyRunner(RunnerBase):
    ...
    ...     def run(self):
    ...         print("Running task:", self.task_name)
    """

    config: EnvConfig
    input: InputConfig
    state: State = field(init=False)
    state_class = LocalState

    def __post_init__(self):
        if isinstance(self.config, dict):
            self.config = EnvConfig(**self.config)
        if isinstance(self.input, dict):
            self.input = InputConfig(**self.input)
        self.state = self.state_class(self.input.cache_key)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        key = re.sub(r"runner$", "", cls.__name__.lower())
        if key in runners:
            warn(f"Runner {key} already exists, overwriting with {cls.__name__}", UserWarning, stacklevel=2)
        runners[key] = cls

    @property
    def clsname(self):
        return self.__class__.__name__.lower()

    @property
    def prefix(self):
        return f"{self.config.s3_prefix}/exec/{self.clsname}"

    def _fmt(self, msg: str) -> str:
        logger.info(msg)
        return f"{self.clsname} [{self.input.cache_key}] :: {msg}"

    def put_state(self, state):
        self.state.put(state)

    def run(self) -> tuple[Union[str, None], str]:
        """Run the task and return the result.

        This method handles acquiring the job lock, updating the state, and
        returning the response and message. The main logic of the task is
        implemented in the `update` method, which must be defined by subclasses.
        """
        state = self.state.get()
        if state is None:
            return None, self._fmt("Could not acquire job lock")
        delete = False
        try:
            logger.info("getting info from %r", self.state_class.__name__)
            new_state, msg, response = self.update(state)
            if new_state is None:
                delete = True
            else:
                self.put_state(new_state)
            return response, self._fmt(msg)
        except Exception:
            delete = True
            raise
        finally:
            if delete:
                if not os.getenv("DML_NO_GC"):
                    self.gc(state)
                self.state.delete()
            else:
                self.state.unlock()

    def update(self, state) -> tuple[dict | None, str, str]:
        """Update the state and return the new state, message, and response.

        The `gc` method is called if and only if the returned state is None.
        """
        raise NotImplementedError("Runner.update must be implemented by subclasses")

    def gc(self, state):
        """Clean up any resources."""
        pass

    def funkify(self, *args, **kwargs):
        """Convert the runner to a function-like interface."""
        raise NotImplementedError("Runner.funkify must be implemented by subclasses")
