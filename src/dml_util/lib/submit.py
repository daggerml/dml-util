import json
import logging
import os
import signal
import subprocess
import sys
import threading
import traceback
from dataclasses import dataclass, field
from time import time
from typing import TextIO
from uuid import uuid4

import boto3
from botocore.exceptions import NoCredentialsError, NoRegionError

from dml_util.aws import get_client

logger = logging.getLogger(__name__)

@dataclass
class Streamer:
    log_group_name: str
    log_stream_name: str
    fd: TextIO
    run_id: str = field(default_factory=lambda: str(uuid4()))
    send_interval: float = field(default=5.0)
    max_events: int = field(default=10000)
    log_buffer: list = field(default_factory=list)
    buffer_lock: threading.Lock = field(default_factory=threading.Lock)
    thread: threading.Thread = field(init=False)
    stop: threading.Event = field(default_factory=threading.Event)
    client: boto3.client = field(default=None)

    def __post_init__(self):
        if self.client is None:
            try:
                # FIXME: remove this when `get_client` is updated to handle regions
                self.client = get_client("logs")
            except NoRegionError:
                logger.warning(f"*** No AWS region configured for {self.run_id} ***")
                self.client = None
                self.stop.set()
        self.thread = threading.Thread(target=self._send_logs)

    def _send(self):
        with self.buffer_lock:
            events = [self.log_buffer.pop(0) for _ in range(min(len(self.log_buffer), self.max_events))]
        if len(events) == 0:
            return
        try:
            self.client.put_log_events(
                logGroupName=self.log_group_name,
                logStreamName=self.log_stream_name,
                logEvents=events,
            )
        except KeyboardInterrupt:
            raise
        except Exception:
            logger.exception(f"Error sending logs for {self.run_id} to {self.log_group_name}/{self.log_stream_name}")

    def _send_logs(self):
        while not self.stop.is_set():
            if self.stop.wait(self.send_interval):
                break
            self._send()
        while len(self.log_buffer) > 0:
            self._send()

    def put(self, message: str):
        logger.info(f"[{self.run_id}]: {message}")
        if not self.stop.is_set():
            with self.buffer_lock:
                self.log_buffer.append({"timestamp": int(time() * 1000), "message": message})

    def run(self):
        try:
            self.client.create_log_stream(logGroupName=self.log_group_name, logStreamName=self.log_stream_name)
        except NoCredentialsError:
            logger.warning(f"*** No CloudWatch client available for {self.run_id} ***")
            self.stop.set()
        except self.client.exceptions.ResourceAlreadyExistsException:
            pass
        if not self.stop.is_set():
            self.thread.start()
        try:
            self.put(f"*** Starting {self.run_id} ***")
            for line in iter(self.fd.readline, ""):
                if not line.strip():
                    continue
                self.put(line.strip())
            self.put(f"*** Ending {self.run_id} ***")
        except Exception as e:
            self.put(f"*** Error in {self.run_id}: {e} ***"),
            [self.put(line) for line in traceback.format_exc().splitlines()]
            self.put(f"** Ending {self.run_id} due to error ***")
        finally:
            self.stop.set()
            self.join()

    def join(self):
        if self.thread.is_alive():
            self.thread.join()


def _run_and_stream(command=None, run_id=None, log_group=None, out_stream=None, err_stream=None):
    command = command or json.loads(os.environ.pop("DML_CMD"))
    run_id = run_id or os.environ["DML_RUN_ID"]
    log_group = log_group or os.environ["DML_LOG_GROUP"]
    out_stream = out_stream or os.environ["DML_LOG_STDOUT"]
    err_stream = err_stream or os.environ["DML_LOG_STDERR"]

    def start_streamer(stream_name, fd):
        streamer = Streamer(log_group, stream_name, fd, run_id)
        thread = threading.Thread(target=streamer.run)
        thread.start()
        return thread, streamer

    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    out_thread, out_str = start_streamer(out_stream, process.stdout)
    err_thread, err_str = start_streamer(err_stream, process.stderr)

    def stop():
        out_str.stop.set()
        err_str.stop.set()
        out_str.thread.join()
        err_str.thread.join()
        out_thread.join()
        err_thread.join()

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)
    try:
        process.wait()
    finally:
        out_thread.join()
        err_thread.join()


def launch_detached(cmd, env=None):
    """
    Fire-and-forget.  Returns immediately.  The background helper
    keeps running after *this* script exits.

        launch_detached(["python", "train.py"], "my-logs", "exec/42")
    """
    for k, v in (env or {}).items():
        if not isinstance(k, str):
            raise TypeError(f"Environment variable {k!r} must be strings, got {k!r}")
        if not isinstance(v, str):
            raise TypeError(f"Environment variable values must be strings, got {v!r}")
    proc = subprocess.Popen(
        [sys.executable, "-u", __file__, "--logstream-worker"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
        close_fds=True,
        env={**os.environ, **(env or {}), "DML_CMD": json.dumps(cmd)},
    )
    return proc.pid


if __name__ == "__main__":
    _run_and_stream() if "--logstream-worker" in sys.argv else None
