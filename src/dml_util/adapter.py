import json
import logging
import logging.config
import os
import re
import sys
from argparse import ArgumentParser
from dataclasses import dataclass
from time import sleep
from urllib.parse import urlparse
from uuid import uuid4

from daggerml import Error, Resource

from dml_util.baseutil import S3Store, add_log_stream, get_client

logger = logging.getLogger(__name__)

try:
    import watchtower
except ImportError:
    watchtower = None


def maybe_get_client(name):
    try:
        return get_client(name)
    except Exception:
        logger.debug("failed to get client %s", name)
        return None


def _read_data(file):
    if not isinstance(file, str):
        return file.read()
    if urlparse(file).scheme == "s3":
        return S3Store().get(file).decode()
    with open(file) as f:
        data = f.read()
    return data.strip()


def _write_data(data, to, mode="w"):
    if not isinstance(to, str):
        return print(data, file=to, flush=True)
    if urlparse(to).scheme == "s3":
        return S3Store().put(data.encode(), uri=to)
    with open(to, mode) as f:
        f.write(data + ("\n" if mode == "a" else ""))
        f.flush()


@dataclass
class Adapter:
    ADAPTERS = {}

    @classmethod
    def cli_setup(cls, debug=False):
        cache_key = os.environ["DML_CACHE_KEY"]
        os.environ["DML_LOG_GROUP"] = os.getenv("DML_LOG_GROUP", "dml")
        run_id = os.environ["DML_RUN_ID"] = os.getenv("DML_RUN_ID", uuid4().hex[:8])
        debug = debug or os.getenv("DML_DEBUG")
        _config = {
            "version": 1,
            "disable_existing_loggers": False,
            "formatters": {
                "simple": {
                    "format": f"[{cls.__name__.lower()} {run_id}] %(levelname)1s %(name)s: %(message)s",
                }
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "formatter": "simple",
                    "level": (logging.DEBUG if debug else logging.WARNING),
                }
            },
            "loggers": {
                "dml_util": {
                    "handlers": ["console"],
                    "level": logging.DEBUG,
                },
                "": {
                    "handlers": ["console"],
                    "level": logging.WARNING,
                },
            },
        }
        if watchtower:
            _config["handlers"]["cloudwatch"] = {
                "class": "watchtower.CloudWatchLogHandler",
                "log_group_name": os.environ["DML_LOG_GROUP"],
                "log_stream_name": add_log_stream(f"/run/{cache_key}/adapter"),
                "formatter": "simple",
                "create_log_stream": True,
                "create_log_group": False,
                "level": logging.DEBUG,
            }
            _config["loggers"]["dml_util"]["handlers"].append("cloudwatch")
            _config["loggers"][""]["handlers"].append("cloudwatch")
        logging.config.dictConfig(_config)

    @classmethod
    def cli_teardown(cls):
        # find the watchtower handler and flush/close it so we don't wait
        if watchtower:
            for handler in logging.getLogger("dml_util").handlers:
                if isinstance(handler, watchtower.CloudWatchLogHandler):
                    logging.getLogger("dml_util").removeHandler(handler)
                    logging.getLogger("").removeHandler(handler)
                    handler.flush()
                    handler.close()

    @classmethod
    def cli(cls, args=None):
        if args is None:
            parser = ArgumentParser()
            parser.add_argument("uri")
            parser.add_argument("-i", "--input", default=sys.stdin)
            parser.add_argument("-o", "--output", default=sys.stdout)
            parser.add_argument("-e", "--error", default=sys.stderr)
            parser.add_argument("-n", "--n-iters", default=1, type=int)
            parser.add_argument("--debug", action="store_true")
            args = parser.parse_args()
        cls.cli_setup(debug=args.debug)
        try:
            n_iters = args.n_iters if args.n_iters > 0 else float("inf")
            logger.debug("reading data from %r", args.input)
            input = _read_data(args.input)
            while n_iters > 0:
                resp, msg = cls.send_to_remote(args.uri, input)
                _write_data(msg, args.error, mode="a")
                if resp:
                    _write_data(resp, args.output)
                    return 0
                n_iters -= 1
                if n_iters > 0:
                    sleep(0.2)
            return 0
        except Exception as e:
            logger.exception("Error in adapter")
            try:
                _write_data(str(Error(e)), args.error)
            except Exception:
                logger.exception("cannot write to %r", args.error)
            return 1
        finally:
            cls.cli_teardown()

    @classmethod
    def funkify(cls, uri, data):
        return Resource(uri, data=data, adapter=cls.ADAPTER)

    @classmethod
    def register(cls, def_cls):
        cls.ADAPTERS[re.sub(r"adapter$", "", def_cls.__name__.lower())] = def_cls
        return def_cls

    @classmethod
    def send_to_remote(cls, uri, data):
        raise NotImplementedError("send_to_remote not implemented for this adapter")


@Adapter.register
@dataclass
class LambdaAdapter(Adapter):
    ADAPTER = "dml-util-lambda-adapter"
    CLIENT = maybe_get_client("lambda")

    @classmethod
    def send_to_remote(cls, uri, data):
        response = cls.CLIENT.invoke(
            FunctionName=uri,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=data.strip().encode(),
        )
        payload = response["Payload"].read()
        payload = json.loads(payload)
        if payload.get("status", 400) // 100 in [4, 5]:
            status = payload.get("status", 400)
            raise Error(
                f"lambda returned with bad status: {status}\n{payload.get('message')}",
                context=payload,
                code=f"status:{status}",
            )
        out = payload.get("response", {})
        return out, payload.get("message")


@Adapter.register
class LocalAdapter(Adapter):
    ADAPTER = "dml-util-local-adapter"
    RUNNERS = {}

    @classmethod
    def funkify(cls, uri, data):
        data = cls.RUNNERS[uri].funkify(**data)
        if isinstance(data, tuple):
            uri, data = data
        return super().funkify(uri, data)

    @classmethod
    def register(cls, def_cls):
        cls.RUNNERS[re.sub(r"runner$", "", def_cls.__name__.lower())] = def_cls
        return def_cls

    @classmethod
    def send_to_remote(cls, uri, data):
        runner = cls.RUNNERS[uri](**json.loads(data))
        return runner.run()
