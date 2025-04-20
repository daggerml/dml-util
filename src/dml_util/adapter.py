import json
import logging
import os
import re
import sys
from argparse import ArgumentParser
from dataclasses import dataclass
from time import sleep
from urllib.parse import urlparse

from daggerml import Error, Resource

from dml_util.baseutil import S3Store, get_client

logger = logging.getLogger(__name__)


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
    def cli(cls, args=None):
        if args is None:
            parser = ArgumentParser()
            parser.add_argument("uri")
            parser.add_argument("-i", "--input", default=sys.stdin)
            parser.add_argument("-o", "--output", default=sys.stdout)
            parser.add_argument("-e", "--error", default=sys.stderr)
            parser.add_argument("-n", "--n-iters", default=1, type=int)
            args = parser.parse_args()
            if os.getenv("DML_DEBUG"):
                logging.basicConfig(level=logging.DEBUG)
            else:
                logging.basicConfig(level=logging.WARNING)
        try:
            n_iters = args.n_iters if args.n_iters > 0 else float("inf")
            logger.debug("reading data from %r", args.input)
            input = _read_data(args.input)
            while n_iters > 0:
                resp, msg = cls.send_to_remote(args.uri, input)
                _write_data(msg, args.error, mode="a")
                if resp.get("dump"):
                    _write_data(json.dumps(resp), args.output)
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
    CLIENT = get_client("lambda")

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
                "lambda returned with bad status",
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
