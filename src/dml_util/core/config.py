"""Configuration management for DaggerML utilities.

DaggerML adapters are configured in two main ways:
1. Environment variables: The `EnvConfig` class loads configuration from environment variables.
2. Input data: Typically passed in via stdin, the `InputConfig` class stores the data passed from dml
"""

import json
import os
from dataclasses import asdict, dataclass
from typing import Union
from uuid import uuid4

from dml_util.core.utils import js_dump


@dataclass
class EnvConfig:
    """ENVVARS for a DaggerML run.

    This class is used to configure the DaggerML adapter and is typically specified via environment variables.
    """

    s3_bucket: str
    """S3 bucket for the data store."""
    s3_prefix: str
    """S3 prefix for the data store."""
    log_group: str
    """Log group for the current function (executor and jobs will log to here)."""
    run_id: str
    """UUID identifying this run."""
    debug: bool
    """Debug flag, if set to True, will enable debug logging and other debug features."""

    @classmethod
    def from_env(cls, debug: bool = False) -> "EnvConfig":
        "Load configuration from environment variables."
        self = cls(
            s3_bucket=os.environ["DML_S3_BUCKET"],
            s3_prefix=os.environ["DML_S3_PREFIX"],
            log_group=os.environ.get("DML_LOG_GROUP", "dml"),
            run_id=os.environ.get("DML_RUN_ID", uuid4().hex[:8]),
            debug=bool(os.environ.get("DML_DEBUG")) or debug,
        )
        return self

    def to_dict(self) -> dict:
        """Convert the configuration to a dictionary."""
        return asdict(self)

    def dumps(self) -> str:
        """Serialize the configuration to a JSON string."""
        return js_dump(self.to_dict())

    def to_envvars(self) -> dict:
        """Convert the configuration to environment variables."""
        out = {f"DML_{k.upper()}": v for k, v in self.to_dict().items() if k != "debug"}
        if self.debug:
            out["DML_DEBUG"] = "1"
        return out

    @classmethod
    def loads(cls, data: str) -> "EnvConfig":
        """Deserialize the configuration from a JSON string."""
        return cls(**json.loads(data))


@dataclass
class InputConfig:
    """Configuration for input data.

    This class is used to specify the input data configuration for a DaggerML run.
    """

    cache_path: str
    """Path to the cache directory."""
    cache_key: str
    """The execution's cache key."""
    kwargs: Union[dict, None]
    """The function's specific data. May include a sub-adapter and URI."""
    dump: str
    """The dag dump, an opaque blob serialized as a string."""

    def get_sub(self):
        """Get sub-adapter, URI, and kwargs for the sub function's stdin."""
        assert isinstance(self.kwargs, dict), "cannot get sub from non-dict kwargs"
        sub = self.kwargs["sub"]
        ks = {
            "cache_path": self.cache_path,
            "cache_key": self.cache_key,
            "kwargs": sub["data"],
            "dump": self.dump,
        }
        return sub["adapter"], sub["uri"], js_dump(ks)
