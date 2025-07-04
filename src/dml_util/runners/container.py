"""Container-based execution runners.

This module provides runners for executing tasks in container environments,
such as Docker. These runners can execute commands in isolated containers
with specific images and configurations.
"""

import logging
import os
import shlex
import subprocess
from tempfile import mkdtemp
from textwrap import dedent

from dml_util.core.utils import _run_cli, if_read_file, proc_exists
from dml_util.runners.base import RunnerBase

logger = logging.getLogger(__name__)


class DockerRunner(RunnerBase):
    """Runs a command in a Docker container."""

    _file_names = ("stdin.dump", "stdout.dump", "stderr.dump")

    @classmethod
    def funkify(cls, image, sub, docker_path=None, flags=None):
        return {
            "sub": sub,
            "image": image,
            "flags": flags or [],
            "docker_path": docker_path,
        }

    def _dkr(self, *args, **kwargs):
        dkr = self.input.kwargs.get("docker_path") or "docker"
        return _run_cli([dkr, *args], **kwargs)

    def start_docker(self, tmpd, sub_adapter, sub_uri):
        # FIXME: remove this method and put this in `submit`.
        envs = [("-e", f"{k}={v}") for k, v in self.config.to_envvars().items()]
        envs = [x for y in envs for x in y]
        return self._dkr(
            "run",
            "-v",
            f"{tmpd}:{tmpd}",
            "-d",
            *self.input.kwargs.get("flags", []),
            *envs,
            self.input.kwargs["image"]["uri"],
            sub_adapter,
            "-n",
            "-1",
            "--debug",
            "-i",
            f"{tmpd}/{self._file_names[0]}",
            "-o",
            f"{tmpd}/{self._file_names[1]}",
            "-e",
            f"{tmpd}/{self._file_names[2]}",
            sub_uri,
        )

    def get_docker_status(self, cid):
        return self._dkr("inspect", "-f", "{{.State.Status}}", cid, check=False) or "no-longer-exists"

    def get_docker_exit_code(self, cid):
        return int(self._dkr("inspect", "-f", "{{.State.ExitCode}}", cid))

    def get_docker_logs(self, cid):
        return self._dkr("logs", cid, check=False)

    def submit(self):
        sub_adapter, sub_uri, sub_kwargs = self.input.get_sub()
        tmpd = mkdtemp(prefix="dml.")
        with open(f"{tmpd}/{self._file_names[0]}", "w") as f:
            f.write(sub_kwargs)
        container_id = self.start_docker(tmpd, sub_adapter, sub_uri)
        return container_id, tmpd

    def update(self, state):
        cid = state.get("cid")
        if cid is None:
            cid, tmpd = self.submit()
            return {"cid": cid, "tmpd": tmpd}, f"container {cid} started", None
        status = self.get_docker_status(cid)
        dkr_logs = self.get_docker_logs(cid)
        if status in ["created", "running"]:
            return state, f"container {cid} running -- {dkr_logs}", None
        tmpd = state["tmpd"]
        msg = f"container {cid} finished with status {status!r}"
        result = if_read_file(f"{tmpd}/{self._file_names[1]}")
        if result:
            return state, msg, result
        error_str = if_read_file(f"{tmpd}/{self._file_names[2]}") or ""
        exit_code = self.get_docker_exit_code(cid)
        msg = dedent(
            f"""
            Docker job {self.input.cache_key}
              {msg}
              exit code {exit_code}
              No output written
              STDERR:
                {error_str}
              STDOUT:
                {result}
            ================
            """
        ).strip()
        raise RuntimeError(msg)

    def gc(self, state):
        if "cid" in state:
            _run_cli(["docker", "rm", state["cid"]], check=False)
        if "tmpd" in state:
            _run_cli(["rm", "-r", state["tmpd"]], check=False)
        super().gc(state)


class Test(DockerRunner):
    """A test runner that simulates Docker for testing."""

    def start_docker(self, flags, image_uri, *sub_cmd):
        env = {k: v for k, v in os.environ.items() if not k.startswith("DML_")}
        for i, flag in enumerate(flags):
            if flag == "-v":
                tmpfrom, tmpto = flags[i + 1].split(":")
        for i, flag in enumerate(flags):
            if flag == "-e":
                a, b = flags[i + 1].split("=")
                env[a] = b.replace(tmpto, tmpfrom)
        env["DML_FN_CACHE_DIR"] = image_uri
        sub_cmd = [x.replace(tmpto, tmpfrom) for x in sub_cmd]
        proc = subprocess.Popen(
            sub_cmd,
            stdout=open(f"{tmpfrom}/stdout", "w"),
            stderr=open(f"{tmpfrom}/stderr", "w"),
            start_new_session=True,
            text=True,
            env=env,
        )
        return [proc.pid, tmpfrom]

    def get_docker_status(self, cid):
        return "running" if proc_exists(cid[0]) else "exited"

    def get_docker_exit_code(self, cid):
        return 0

    def get_docker_logs(self, cid):
        stdout = if_read_file(f"{cid[1]}/stdout")
        stderr = if_read_file(f"{cid[1]}/stderr")
        return {"stdout": stdout, "stderr": stderr}

    def gc(self, state):
        if "cid" in state:
            _run_cli(["kill", "-9", str(state["cid"][0])], check=False)
        if "tmpd" in state:
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            _run_cli(command, shell=True)
        state["cid"] = "doesnotexist"
        super().gc(state)
