import json
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
    client: boto3.client = field(default_factory=lambda: boto3.client("logs"))

    def __post_init__(self):
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
        except Exception:
            pass

    def _send_logs(self):
        while not self.stop.is_set():
            if self.stop.wait(self.send_interval):
                break
            self._send()
        while len(self.log_buffer) > 0:
            self._send()

    def put(self, *messages: str):
        with self.buffer_lock:
            self.log_buffer.extend([{"timestamp": int(time() * 1000), "message": message} for message in messages])

    def run(self):
        if not self.log_group_name or not self.log_stream_name:
            return
        self.thread.start()
        try:
            try:
                self.client.create_log_stream(logGroupName=self.log_group_name, logStreamName=self.log_stream_name)
            except self.client.exceptions.ResourceAlreadyExistsException:
                pass
            self.put(f"*** Starting {self.run_id} ***")
            for line in iter(self.fd.readline, ""):
                if not line.strip():
                    continue
                self.put(line.strip())
            self.put(f"*** Ending {self.run_id} ***")
        except Exception as e:
            self.put(
                f"*** Error in {self.run_id}: {e} ***",
                *[line for line in traceback.format_exc().splitlines()],
                f"** Ending {self.run_id} due to error ***",
            )
        finally:
            self.stop.set()
            self.thread.join()


def _run_and_stream(command, run_id, log_group, out_stream, err_stream):
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


def _worker():
    _run_and_stream(
        json.loads(os.environ.pop("DML_CMD")),
        os.getenv("DML_RUN_ID"),
        os.getenv("DML_LOG_GROUP"),
        os.getenv("DML_LOG_STDOUT"),
        os.getenv("DML_LOG_STDERR"),
    )


def launch_detached(cmd, env=None):
    """
    Fire-and-forget.  Returns immediately.  The background helper
    keeps running after *this* script exits.

        launch_detached(["python", "train.py"], "my-logs", "exec/42")
    """
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
    _worker() if "--logstream-worker" in sys.argv else None
