"""AWS Lambda adapter.

This module provides an adapter for running DaggerML functions in AWS Lambda.
The Lambda adapter allows running DaggerML functions in AWS Lambda,
passing environment variables and configuration along.
"""

import json
import logging
from dataclasses import dataclass
from typing import Union

from daggerml import Error

from dml_util.adapters.base import AdapterBase
from dml_util.aws import get_client
from dml_util.core.config import EnvConfig

logger = logging.getLogger(__name__)


@dataclass
class LambdaAdapter(AdapterBase):
    """AWS Lambda adapter."""

    ADAPTER = "dml-util-lambda-adapter"

    @classmethod
    def send_to_remote(cls, uri, config: EnvConfig, dump: str) -> tuple[Union[str, None], str]:
        """Send data to a Lambda function.

        Parameters
        ----------
        uri : str
            The Lambda function name.
        config : EnvConfig
            Configuration for the run.
        dump : str
            The payload to send.

        Returns
        -------
        tuple[str | None, str]
            A tuple of (response, message).
        """
        response = get_client("lambda").invoke(
            FunctionName=uri,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=json.dumps({"config": config.to_dict(), "dump": dump}).encode(),
        )
        payload = response["Payload"].read()
        payload = json.loads(payload)
        if payload.get("status", 400) // 100 in [4, 5]:
            status = payload.get("status", 400)
            raise Error(
                payload.get("message", "Lambda returned with error status"),
                origin="lambda",
                type=f"exit:{status}",
                stack=payload.get("stack", []),
            )
        out = payload.get("response")
        msg = payload.get("message", "running" if out is None else "done")
        return out, msg
