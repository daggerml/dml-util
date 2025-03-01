import json
import logging
import os
import shlex
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from time import time

import boto3
from botocore.exceptions import ClientError
from daggerml import Dml, Resource

from dml_util.baseutil import DagExecError, LocalState, Runner, WithDataError

logger = logging.getLogger(__name__)
CFN_EXEC = Resource("dml-util-cfn-exec", adapter="dml-util-local-adapter")
SSH_EXEC = Resource("dml-util-ssh-exec", adapter="dml-util-local-adapter")
SCRIPT_EXEC = Resource("dml-util-script-exec", adapter="dml-util-local-adapter")
DOCKER_EXEC = Resource("dml-util-docker-exec", adapter="dml-util-local-adapter")


@dataclass
class LocalRunner(Runner):
    state_class = LocalState

    def _run(self, command, **kw):
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
            **kw,
        )
        if result.returncode != 0:
            msg = f"{command}\n{result.returncode = }\n{result.stdout}\n\n{result.stderr}"
            raise DagExecError(msg)
        return result.stdout.strip()

    @classmethod
    def cli(cls):
        data = json.loads(sys.stdin.read())
        try:
            dump, msg = cls(**data).run()
        except WithDataError as e:
            print(e, file=sys.stderr)
            sys.exit(1)
        print(msg, file=sys.stderr)
        if dump is not None:
            print(dump)


class Script(LocalRunner):
    def submit(self):
        tmpd = self._run("mktemp -d -t dml.XXXXXX".split())
        with open(f"{tmpd}/script", "w") as f:
            f.write(self.kwargs["script"])
        subprocess.run(["chmod", "+x", f"{tmpd}/script"], check=True)
        with open(f"{tmpd}/input.dump", "w") as f:
            f.write(self.dump)
        env = dict(os.environ).copy()
        env.update(
            {
                "DML_INPUT_LOC": f"{tmpd}/input.dump",
                "DML_OUTPUT_LOC": f"{tmpd}/output.dump",
                **self.env,
            }
        )
        proc = subprocess.Popen(
            [f"{tmpd}/script"],
            stdout=open(f"{tmpd}/stdout", "w"),
            stderr=open(f"{tmpd}/stderr", "w"),
            start_new_session=True,
            text=True,
            env=env,
        )
        return proc.pid, tmpd

    def update(self, state):
        pid = state.get("pid")
        if pid is None:
            pid, tmpd = self.submit()
            return {"pid": pid, "tmpd": tmpd}, f"{pid = } started", None

        def proc_exists(pid):
            try:
                os.kill(pid, 0)
            except ProcessLookupError:
                return False
            except PermissionError:
                return True
            return True

        tmpd = state["tmpd"]
        if proc_exists(pid):
            return state, f"{pid = } running", None
        elif os.path.isfile(f"{tmpd}/output.dump"):
            with open(f"{tmpd}/output.dump") as f:
                return state, f"{pid = } finished", f.read()
        msg = f"{pid = } finished without writing output"
        if os.path.exists(f"{tmpd}/stderr"):
            with open(f"{tmpd}/stderr", "r") as f:
                msg = f"{msg}\nSTDERR:\n-------\n{f.read()}"
        raise RuntimeError(msg)

    def delete(self, state):
        if "pid" in state:
            self._run(f"kill -9 {state['pid']} || echo", shell=True)
        if "tmpd" in state:
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            self._run(command, shell=True)
        super().delete(state)


class Ssh(LocalRunner):
    def __post_init__(self, *args, **kwargs):
        super().__post_init__(*args, **kwargs)
        self.host = self.kwargs["host"]
        if "user" in self.kwargs:
            self.host = f"{self.kwargs['user']}@{self.host}"
        self.flags = ["-o", "BatchMode=yes", *self.kwargs.get("flags", [])]

    def _run(self, command, **kw):
        return super()._run(["ssh", *self.flags, self.host, command], **kw)

    def submit(self):
        xtbl = self.kwargs["executable"]
        path_dir = self.kwargs["path_dir"]
        tmpd = self._run("mktemp -d -t dml.XXXXXX")
        self._run(f"cat > {tmpd}/script", input=self.kwargs["script"])
        self._run(f"chmod +x {tmpd}/script")
        self._run(f"cat > {tmpd}/input.dump", input=self.dump)
        env = {
            "DML_INPUT_LOC": f"{tmpd}/input.dump",
            "DML_OUTPUT_LOC": f"{tmpd}/output.dump",
            "DML_REPO_DIR": "/dev/null",
            **self.env,
        }
        bash_script = "#!/bin/bash\n\n"
        bash_script += f"dml_path={shlex.quote(path_dir)}\n"
        for k, v in env.items():
            bash_script = f"{bash_script}export {k}={shlex.quote(v)}\n"
        bash_script += 'export PATH="$dml_path:$PATH"\n'
        bash_script += f"\nnohup {xtbl!r} {tmpd}/script &> {tmpd}/stdouterr & echo $!"
        self._run(f"cat > {tmpd}/script.sh", input=bash_script)
        # pid = self._run(f"chmod +x {tmpd}/script.sh && {tmpd}/script.sh")
        pid = self._run("/bin/bash", input=bash_script)
        return {"pid": pid, "tmpd": tmpd, "submitted": time()}

    def update(self, state):
        pid = state.get("pid")
        if pid is None:
            state = self.submit()
            return state, f"job {state} started", None
        status = "running"
        t0 = state["submitted"]
        dump = None
        if str(pid) not in self._run(f"ps -p {pid} || echo no"):
            status = "finished"
            tmpd = state["tmpd"]
            dump = self._run(f"cat {tmpd}/output.dump || echo")
            if dump == "":
                msg = self._run(f"cat {tmpd}/stdouterr")
                msg = f"{msg}\n\ntemp files in {tmpd} on {self.host}"
                raise DagExecError(msg)
        return state, f"[{pid} @ {int(time() - t0):1.2E} seconds]:{status}", dump

    def delete(self, state):
        if "pid" in state:
            self._run(f"kill -9 {state['pid']} || echo")
        if "tmpd" in state:
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            self._run(command)
        super().delete(state)


class Docker(LocalRunner):
    def _run_command(self, command):
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            return result.returncode, (result.stdout + result.stderr).strip()
        except subprocess.SubprocessError as e:
            return 1, str(e)

    def submit(self):
        tmpd = self._run("mktemp -d -t dml.XXXXXX".split())
        with open(f"{tmpd}/script", "w") as f:
            f.write(self.kwargs["script"])
        subprocess.run(["chmod", "+x", f"{tmpd}/script"], check=True)
        with open(f"{tmpd}/input.dump", "w") as f:
            f.write(self.dump)
        env_flags = [("-e", f"{k}={v!r}") for k, v in self.env.items()]
        env_flags = [y for x in env_flags for y in x]
        exit_code, container_id = self._run_command(
            [
                "docker",
                "run",
                "-v",
                f"{tmpd}:/opt/dml",
                "-e",
                "DML_INPUT_LOC=/opt/dml/input.dump",
                "-e",
                "DML_OUTPUT_LOC=/opt/dml/output.dump",
                *env_flags,
                "-d",  # detached
                *self.kwargs.get("flags", []),
                self.kwargs["image"],
                "/opt/dml/script",
            ],
        )
        if exit_code != 0:
            msg = f"container {container_id} failed to start"
            raise RuntimeError(msg)
        return container_id, tmpd

    def maybe_complete(self, tmpd, cid, status="???"):
        try:
            if os.path.exists(f"{tmpd}/output.dump"):
                with open(f"{tmpd}/output.dump") as f:
                    return f.read()
            _, exit_code_str = self._run_command(["docker", "inspect", "-f", "{{.State.ExitCode}}", cid])
            exit_code = int(exit_code_str)
            msg = f"""
            job {self.cache_key}
              finished with status {status}
              exit code {exit_code}
              No output written
            """.strip()
            raise RuntimeError(msg)
        finally:
            if os.getenv("DML_DOCKER_CLEANUP") == "1":
                self._run_command(["docker", "rm", cid])

    def update(self, state):
        cid = state.get("cid")
        if cid is None:
            cid, tmpd = self.submit()
            return {"cid": cid, "tmpd": tmpd}, f"container {cid} started", None
        # Check if container exists and get its status
        tmpd = state["tmpd"]
        exit_code, status = self._run_command(["docker", "inspect", "-f", "{{.State.Status}}", cid])
        status = status if exit_code == 0 else "no-longer-exists"
        if status in ["created", "running", "restarting"]:
            return state, f"container {cid} running", None
        elif status in ["exited", "paused", "dead", "no-longer-exists"]:
            msg = f"container {cid} finished with status {status!r}"
            return state, msg, self.maybe_complete(tmpd, cid, status)


class Cfn(LocalRunner):
    def fmt(self, stack_id, status, raw_status):
        return f"{stack_id} : {status} ({raw_status})"

    def describe_stack(self, client, name, StackId):
        try:
            stack = client.describe_stacks(StackName=name)["Stacks"][0]
        except ClientError as e:
            if "does not exist" in str(e):
                return None, None
            raise
        raw_status = stack["StackStatus"]
        state = {"StackId": stack["StackId"], "name": name}
        if StackId is not None and state["StackId"] != StackId:
            raise RuntimeError(f"stack ID changed from {StackId} to {state['StackId']}!")
        if raw_status in ["CREATE_COMPLETE", "UPDATE_COMPLETE"]:
            status = "success"
            state["outputs"] = {o["OutputKey"]: o["OutputValue"] for o in stack.get("Outputs", [])}
        elif raw_status in [
            "ROLLBACK_COMPLETE",
            "ROLLBACK_FAILED",
            "CREATE_FAILED",
            "DELETE_FAILED",
        ]:
            events = client.describe_stack_events(StackName=name)["StackEvents"]
            status = "failed"
            failure_events = [e for e in events if "ResourceStatusReason" in e]
            state["failure_reasons"] = [e["ResourceStatusReason"] for e in failure_events]
            if StackId is not None:  # create failed
                msg = "Stack failed:\n\n" + json.dumps(state, default=str, indent=2)
                raise RuntimeError(msg)
        elif StackId is None:
            raise RuntimeError("cannot create new stack while stack is currently being created")
        else:
            status = "creating"
        return state, self.fmt(state["StackId"], status, raw_status)

    def submit(self, client):
        assert Dml is not None, "dml is not installed..."
        with Dml(data=self.dump) as dml:
            with dml.new(f"fn:{self.cache_key}", f"execution of: {self.cache_key}") as dag:
                name, js, params = dag.argv[1:4].value()
        old_state, msg = self.describe_stack(client, name, None)
        fn = client.create_stack if old_state is None else client.update_stack
        try:
            resp = fn(
                StackName=name,
                TemplateBody=json.dumps(js),
                Parameters=[{"ParameterKey": k, "ParameterValue": v} for k, v in params.items()],
                Capabilities=["CAPABILITY_IAM", "CAPABILITY_NAMED_IAM"],
            )
        except ClientError as e:
            if not e.response["Error"]["Message"].endswith("No updates are to be performed."):
                raise
            resp = old_state
        state = {"name": name, "StackId": resp["StackId"]}
        msg = self.fmt(state["StackId"], "creating", None)
        return state, msg

    def update(self, state):
        assert Dml is not None, "dml is not installed..."
        client = boto3.client("cloudformation")
        result = None
        if state == {}:
            state, msg = self.submit(client)
        else:
            state, msg = self.describe_stack(client, **state)
        if "outputs" in state:

            def _handler(dump):
                nonlocal result
                result = dump

            try:
                with Dml(data=self.dump, message_handler=_handler) as dml:
                    with dml.new(f"fn:{self.cache_key}", f"execution of: {self.cache_key}") as dag:
                        for k, v in state["outputs"].items():
                            dag[k] = v
                        dag.stack_id = state["StackId"]
                        dag.stack_name = state["name"]
                        dag.result = state["outputs"]
            except KeyboardInterrupt:
                raise
            except Exception:
                pass
        return state, msg, result


@contextmanager
def aws_fndag():
    import os
    from urllib.parse import urlparse

    assert Dml is not None, "dml is not installed..."

    def _get_data():
        indata = os.environ["DML_INPUT_LOC"]
        p = urlparse(indata)
        if p.scheme == "s3":
            return boto3.client("s3").get_object(Bucket=p.netloc, Key=p.path[1:])["Body"].read().decode()
        with open(indata) as f:
            return f.read()

    def _handler(dump):
        outdata = os.environ["DML_OUTPUT_LOC"]
        p = urlparse(outdata)
        if p.scheme == "s3":
            return boto3.client("s3").put_object(Bucket=p.netloc, Key=p.path[1:], Body=dump.encode())
        with open(outdata, "w") as f:
            f.write(dump)

    with Dml(data=_get_data(), message_handler=_handler) as dml:
        with dml.new("test", "test") as dag:
            yield dag
