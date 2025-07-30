"""Remote execution runners.

This module provides runners for executing tasks in remote environments,
such as SSH. These runners can execute commands on remote machines
with specific configurations.
"""

import logging
import shlex
import subprocess

from dml_util.runners.base import RunnerBase

logger = logging.getLogger(__name__)

SCRIPT_TPL = """
#!/usr/bin/env bash
set -euo pipefail

# REPLACE THIS LINE

# require exactly 2 args
if [ "$#" -ne 2 ]; then
  echo "Usage: echo data | $0 adapter uri" >&2
  exit 1
fi
cmd=( "$@" )
exec "${cmd[@]}"
""".strip()


class SshRunner(RunnerBase):
    """Runs a command over SSH."""

    @classmethod
    def funkify(cls, host, sub, flags=None, env_files=None):
        script = SCRIPT_TPL
        if env_files is not None:
            script = script.replace(
                "REPLACE THIS LINE",
                "\n".join(["ENV FILES HERE..."] + [f". {env_file}" for env_file in env_files]),
            )
        return {"sub": sub, "host": host, "flags": flags or [], "script": script}

    def proc_script(self) -> str:
        # for k, v in self.env set flag in the script
        tmpf, _ = self._run_cmd("mktemp", "-t", "dml.XXXXXX.sh")
        shbang, *lines = self.input.kwargs["script"].split("\n")
        env_lines = [f"export {k}={shlex.quote(v)}" for k, v in self.config.to_envvars().items()]
        script = "\n".join([shbang, *env_lines, *lines])
        self._run_cmd("cat", ">", tmpf, input=script)
        self._run_cmd("chmod", "+x", tmpf)
        return tmpf

    def _run_cmd(self, *user_cmd, **kw):
        cmd = ["ssh", *self.input.kwargs["flags"], self.input.kwargs["host"], *user_cmd]
        logger.debug("Running SSH command: %r", " ".join(cmd))
        resp = subprocess.run(cmd, capture_output=True, text=True, check=False, **kw)
        if resp.returncode != 0:
            msg = f"Ssh(code:{resp.returncode}) {user_cmd}\nSTDOUT\n{resp.stdout}\n\nSTDERR\n{resp.stderr}"
            raise RuntimeError(msg)
        stderr = resp.stderr.strip()
        logger.debug(f"SSH STDERR: {stderr}")
        return resp.stdout.strip(), stderr

    def run(self):
        sub_adapter, sub_uri, sub_kwargs = self.input.get_sub()
        tmpf = self.proc_script()
        stdout, stderr = self._run_cmd(tmpf, sub_adapter, sub_uri, input=sub_kwargs)
        # stdout = json.loads(stdout or "{}")
        self._run_cmd("rm", tmpf)
        return stdout, stderr
