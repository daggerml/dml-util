import argparse
import json
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class Runner:
    name = "?"

    def __init__(self, data):
        self.cache_key = data["cache_key"]
        self.kwargs = data["kwargs"]
        self.dump = data["dump"]
        if "DML_FN_CACHE_LOC" in os.environ:
            self.cache_dir = os.environ["DML_FN_CACHE_LOC"]
        elif "DML_FN_CACHE_DIR" in os.environ:
            config_dir = os.environ["DML_FN_CACHE_DIR"]
            self.cache_dir = f"{config_dir}/cache/dml-util/{self.cache_key}"
        else:
            status = subprocess.run(["dml", "status"], check=True, capture_output=True)
            config_dir = json.loads(status.stdout.decode())["config_dir"]
            self.cache_dir = f"{config_dir}/cache/dml-util/{self.cache_key}"
        os.makedirs(self.cache_dir, exist_ok=True)
        self.state_file = Path(self.cache_dir) / "status"

    def put_state(self, state):
        status_data = {
            "state": state,
            "timestamp": time.time(),
        }
        with open(self.state_file, "w") as f:
            json.dump(status_data, f)

    def get_state(self):
        if not self.state_file.exists():
            return None
        with open(self.state_file, "r") as f:
            return json.load(f)["state"]

    def del_state(self):
        if os.path.exists(self.state_file):
            os.unlink(self.state_file)

    def run(self):
        state = self.get_state()
        state, msg, dump = self.update(state)
        self.del_state() if state is None else self.put_state(state)
        return msg, dump

    @classmethod
    def cli(cls):
        parser = argparse.ArgumentParser()
        parser.add_argument("-d", "--data", default="-", type=argparse.FileType("r"))
        args = parser.parse_args()
        data = json.loads(args.data.read())
        self = cls(data)
        msg, dump = self.run()
        msg = f"{self.name} [{self.cache_key}] :: {msg}"
        print(msg, file=sys.stderr)
        if dump is not None:
            print(dump)


class ScriptRunner(Runner):
    name = "script"

    def submit(self):
        with open(f"{self.cache_dir}/script", "w") as f:
            f.write(self.kwargs["script"][-1])
        subprocess.run(["chmod", "+x", f"{self.cache_dir}/script"], check=True)
        with open(f"{self.cache_dir}/input.dump", "w") as f:
            f.write(self.dump)
        env = dict(os.environ).copy()
        env.update(
            {
                "DML_INPUT_LOC": f"{self.cache_dir}/input.dump",
                "DML_OUTPUT_LOC": f"{self.cache_dir}/output.dump",
            }
        )
        proc = subprocess.Popen(
            [f"{self.cache_dir}/script"],
            stdout=open(f"{self.cache_dir}/stdout", "w"),
            stderr=open(f"{self.cache_dir}/stderr", "w"),
            start_new_session=True,
            text=True,
            env=env,
        )
        return proc.pid

    def update(self, pid):
        if pid is None:
            pid = self.submit()
            return pid, f"{pid = } started", None

        def proc_exists(pid):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            return True

        if proc_exists(pid):
            return pid, f"{pid = } running", None
        elif os.path.isfile(f"{self.cache_dir}/output.dump"):
            with open(f"{self.cache_dir}/output.dump") as f:
                return None, f"{pid = } finished", f.read()
        msg = f"{pid = } finished without writing output"
        if os.path.exists(f"{self.cache_dir}/stderr"):
            with open(f"{self.cache_dir}/stderr", "r") as f:
                msg = f"{msg}\nSTDERR:\n-------\n{f.read()}"
        raise RuntimeError(msg)


class DockerRunner(Runner):
    name = "dkr"

    def _run_command(self, command):
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            return result.returncode, (result.stdout + result.stderr).strip()
        except subprocess.SubprocessError as e:
            return 1, str(e)

    def submit(self):
        with open(f"{self.cache_dir}/script", "w") as f:
            f.write(self.kwargs["script"][-1])
        subprocess.run(["chmod", "+x", f"{self.cache_dir}/script"], check=True)
        with open(f"{self.cache_dir}/input.dump", "w") as f:
            f.write(self.dump)
        exit_code, container_id = self._run_command(
            [
                "docker",
                "run",
                "-v",
                f"{self.cache_dir}:/opt/dml",
                "-e",
                "DML_INPUT_LOC=/opt/dml/input.dump",
                "-e",
                "DML_OUTPUT_LOC=/opt/dml/output.dump",
                "-d",  # detached
                *self.kwargs.get("flags", []),
                self.kwargs["image"][-1],
                "/opt/dml/script",
            ],
        )
        if exit_code != 0:
            msg = f"container {container_id} failed to start"
            raise RuntimeError(msg)
        return container_id

    def maybe_complete(self, container_id, container_status="???"):
        try:
            if os.path.exists(f"{self.cache_dir}/output.dump"):
                with open(f"{self.cache_dir}/output.dump") as f:
                    return f.read()
            _, exit_code_str = self._run_command(["docker", "inspect", "-f", "{{.State.ExitCode}}", container_id])
            exit_code = int(exit_code_str)
            msg = f"""
            job {self.cache_key}
              finished with status {container_status}
              exit code {exit_code}
              No output written
            """.strip()
            raise RuntimeError(msg)
        finally:
            if os.getenv("DML_DOCKER_CLEANUP") == "1":
                self._run_command(["docker", "rm", container_id])

    def update(self, container_id):
        if container_id is None:
            container_id = self.submit()
            return container_id, f"container {container_id} started", None
        # Check if container exists and get its status
        exit_code, container_status = self._run_command(["docker", "inspect", "-f", "{{.State.Status}}", container_id])
        container_status = container_status if exit_code == 0 else "no-longer-exists"
        if container_status in ["created", "running", "restarting"]:
            return container_id, f"container {container_id} running", None
        elif container_status in ["exited", "paused", "dead", "no-longer-exists"]:
            msg = f"container {container_id} finished with status {container_status!r}"
            return None, msg, self.maybe_complete(container_id, container_status)
