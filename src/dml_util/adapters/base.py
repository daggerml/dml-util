"""Base adapter class.

This module provides the base adapter class for the DaggerML utilities.
Adapters are used to connect DaggerML to various execution environments,
such as AWS Lambda or local runners.
"""

import logging
import logging.config
import re
import sys
import time
from argparse import ArgumentParser
from dataclasses import dataclass
from typing import Union
from urllib.parse import urlparse

from daggerml import Error, Resource
from daggerml.core import to_json

from dml_util.aws import get_client
from dml_util.aws.s3 import S3Store
from dml_util.core.config import EnvConfig

logger = logging.getLogger(__name__)

try:
    import watchtower

    class SafeCloudWatchLogHandler(watchtower.CloudWatchLogHandler):
        def __init__(self, *args, boto3_client=None, **kwargs):
            """Initialize the CloudWatch Log Handler with a safe region detection."""
            self._enabled = False
            boto3_client = boto3_client or get_client("logs")
            try:
                super().__init__(*args, boto3_client=boto3_client, **kwargs)
                self._enabled = True
            except Exception as e:
                logger.info(f"CloudWatch logging disabled: {e}")

        def emit(self, record):
            if not self._enabled:
                return
            try:
                super().emit(record)
            except Exception as e:
                logger.debug(f"Failed to emit to CloudWatch: {e}")
except ModuleNotFoundError:
    watchtower = None


def _read_data(file):
    """Read data from a file, stdin, or S3."""
    if not isinstance(file, str):
        return file.read()
    if urlparse(file).scheme == "s3":
        return S3Store().get(file).decode()
    with open(file) as f:
        data = f.read()
    return data.strip()


def _write_data(data, to, mode="w"):
    """Write data to a file, stdout, or S3."""
    if not isinstance(to, str):
        return print(data, file=to, flush=True)
    if urlparse(to).scheme == "s3":
        return S3Store().put(data.encode(), uri=to)
    with open(to, mode) as f:
        f.write(data + ("\n" if mode == "a" else ""))
        f.flush()


class VerboseArgumentParser(ArgumentParser):
    def error(self, message):
        # Customize this however you want
        self.print_usage(sys.stderr)
        self.exit(2, f"\nError: {message}\n\nHint: Run with '--help' to see usage and examples.\n")


@dataclass
class AdapterBase:
    """Base class for DaggerML adapters.

    This class provides a CLI interface for executing DaggerML functions iteratively,
    passing environment variables along. It supports different adapters for remote
    execution, such as AWS Lambda or local runners.

    Attributes
    ----------
    ADAPTER : str
        The name of the adapter (to be defined in subclasses).
    ADAPTERS : dict
    """

    ADAPTER = None  # to be defined in subclasses
    ADAPTERS = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        AdapterBase.ADAPTERS[re.sub(r"adapter$", "", cls.__name__.lower())] = cls

    @staticmethod
    def _setup(config):
        """Setup logging configuration for the run."""
        _config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "simple": {
                    "format": f"[{config.run_id}] %(levelname)1s %(name)s: %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "simple",
                    "level": (logging.DEBUG if config.debug else logging.WARNING),
                }
            },
            "loggers": {
                "dml_util": {
                    "handlers": ["console"],
                    "level": logging.DEBUG,
                    "propagate": False,
                },
                "": {
                    "handlers": ["console"],
                    "level": logging.WARNING,
                },
            },
        }
        logging.config.dictConfig(_config)
        if watchtower:
            # get region from boto3
            try:
                logs_client = get_client("logs")
                logs_client.describe_log_streams(logGroupName=config.log_group, logStreamNamePrefix="/adapter", limit=1)
            except Exception as perm_err:
                logger.info(f"CloudWatch logging not enabled due to AWS access error: {perm_err}")
            else:
                handler = SafeCloudWatchLogHandler(
                    log_group_name=config.log_group,
                    boto3_client=logs_client,
                    log_stream_name="/adapter",
                    create_log_stream=True,
                    create_log_group=False,
                    level=logging.DEBUG,
                )
                if handler._enabled:
                    logging.getLogger("dml_util").addHandler(handler)
                    logging.getLogger("").addHandler(handler)
                    logger.debug("added watchtower handler %r", handler)

    @staticmethod
    def _teardown():
        if watchtower:
            for handler in logging.getLogger("dml_util").handlers:
                if isinstance(handler, watchtower.CloudWatchLogHandler):
                    logging.getLogger("").removeHandler(handler)
                    logger.debug("removing watchtower handler %r", handler)
                    logging.getLogger("dml_util").removeHandler(handler)
                    handler.flush()
                    handler.close()

    @classmethod
    def cli(cls, args=None):
        """
        Command-line interface for the adapter.

        This method reads input data from a file or stdin, sends it to a remote service
        specified by the URI, and writes the response to an output file or stdout.
        If an error occurs, it writes the error message to an error file or stderr.

        Cli Parameters
        --------------
        uri : str
            URI of the function to invoke.
        --input FILE, -i FILE : path, optional (default: STDIN)
            Input data file or stdin or s3 location of where to read the dump from.
        --output FILE, -o FILE : path, optional (default: STDOUT)
            Output location for the response data (can be a file, stdout, or s3 location).
        --error FILE, -e FILE : path, optional (default: STDERR)
            Error output location (can be a file, stderr, or s3 location).
        --n-iters N, -n N : int, optional (default: 1)
            Number of iterations to run. Set to 0 to run indefinitely.
        --debug : flag, optional
            Enables debug logging.

        Returns
        -------
        int
            Exit code: 0 on success, 1 on error.
        """
        if args is None:
            parser = VerboseArgumentParser(description=f"DaggerML {cls.__name__} CLI")
            parser.add_argument("uri")
            parser.add_argument("-i", "--input", default=sys.stdin)
            parser.add_argument("-o", "--output", default=sys.stdout)
            parser.add_argument("-e", "--error", default=sys.stderr)
            parser.add_argument("-n", "--n-iters", default=1, type=int)
            parser.add_argument("--debug", action="store_true")
            args = parser.parse_args()
        config = EnvConfig.from_env(debug=args.debug)
        cls._setup(config)
        try:
            n_iters = args.n_iters if args.n_iters > 0 else float("inf")
            logger.debug("reading data from %r", args.input)
            dump = _read_data(args.input)
            while n_iters > 0:
                resp, msg = cls.send_to_remote(args.uri, config, dump)
                logger.debug("response: %r", resp)
                logger.info("message: %r", msg)
                _write_data(msg, args.error, mode="a")
                if resp:
                    _write_data(resp, args.output)
                    return 0
                n_iters -= 1
                if n_iters > 0:
                    time.sleep(0.2)
            return 0
        except Exception as e:
            logger.exception("Error in adapter")
            try:
                _write_data(to_json(Error.from_ex(e)), args.output)
                return 0
            except Exception:
                logger.exception("cannot write to %r", args.output)
            return 1
        finally:
            cls._teardown()

    @classmethod
    def funkify(cls, uri, data):
        return Resource(uri, data=data, adapter=cls.ADAPTER)

    @classmethod
    def send_to_remote(cls, uri: str, config: EnvConfig, dump: str) -> tuple[Union[str, None], str]:
        """Send data to a remote service specified by the URI.

        Parameters
        ----------
        uri : str
            The URI of the remote service.
        config : EnvConfig
            Configuration for the run, including cache path, cache key, S3 bucket, etc.
        dump : str
            The opaque blob to send to the remote service.

        Returns
        -------
        tuple[str, str]
            A tuple containing the response data and a message. If the response is truthy,
            we pass it on to the caller via --output flag.
            The message is written to the --error flag.

        Notes
        -----
        * Any errors raised here will be caught by the CLI and written as the output.
        """
        raise NotImplementedError("send_to_remote not implemented for this adapter")
