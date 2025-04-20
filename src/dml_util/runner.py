import json
import logging
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from textwrap import dedent

import boto3
from botocore.exceptions import ClientError
from daggerml import Dml

from dml_util.adapter import LocalAdapter
from dml_util.baseutil import Runner, S3Store, _run_cli, if_read_file, proc_exists

logger = logging.getLogger(__name__)


@LocalAdapter.register
class ScriptRunner(Runner):
    @classmethod
    def funkify(cls, script, cmd=("python3",), suffix=".py"):
        return {"script": script, "cmd": list(cmd), "suffix": suffix}

    def submit(self):
        logger.warning("Submitting script to local runner")
        print("Submitting script to local runner", file=sys.stderr)
        tmpd = mkdtemp(prefix="dml.")
        script_path = f"{tmpd}/script" + (self.kwargs["suffix"] or "")
        with open(script_path, "w") as f:
            f.write(self.kwargs["script"])
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
            [*self.kwargs["cmd"], script_path],
            stdout=open(f"{tmpd}/stdout", "w"),
            stderr=open(f"{tmpd}/stderr", "w"),
            start_new_session=True,
            text=True,
            env=env,
        )
        return proc.pid, tmpd

    def update(self, state):
        pid = state.get("pid")
        response = {}
        if pid is None:
            pid, tmpd = self.submit()
            return {"pid": pid, "tmpd": tmpd}, f"{pid = } started", response
        tmpd = state["tmpd"]
        if proc_exists(pid):
            return state, f"{pid = } running", response
        dump = if_read_file(f"{tmpd}/output.dump")
        if dump is not None:
            response["dump"] = dump
            s3 = S3Store()
            try:
                logs = {k: f"{tmpd}/{k}" for k in ["stdout", "stderr"]}
                logs = {k: s3.put(filepath=v, suffix=".log").uri for k, v in logs.items() if os.path.isfile(v)}
                response["logs"] = logs
            except Exception:
                pass
            return state, f"{pid = } finished", response
        error = if_read_file(f"{tmpd}/stderr") or ""
        msg = f"[Script] {pid = } finished without writing output\nSTDERR:\n-------\n{error}"
        msg = f"{msg}\n\nSTDOUT:\n-------\n{if_read_file(f'{tmpd}/stdout')}"
        raise RuntimeError(msg)

    def gc(self, state):
        if "pid" in state:
            _run_cli(f"kill -9 {state['pid']} || echo", shell=True)
        if "tmpd" in state:
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            _run_cli(command, shell=True)
        super().gc(state)


@LocalAdapter.register
class WrappedRunner(Runner):
    @classmethod
    def funkify(cls, script, sub):
        kw = {"script": script, "sub": sub}
        return kw

    def get_script_and_args(self):
        sub_uri, sub_kwargs, sub_adapter = self.sub_data()
        return self.kwargs["script"], sub_adapter, sub_uri, sub_kwargs

    def run(self):
        script, sub_adapter, sub_uri, sub_kwargs = self.get_script_and_args()
        with TemporaryDirectory() as tmpd:
            with open(f"{tmpd}/script", "w") as f:
                f.write(script)
            subprocess.run(["chmod", "+x", f"{tmpd}/script"], check=True)
            cmd = [f"{tmpd}/script", sub_adapter, sub_uri]
            result = subprocess.run(
                cmd,
                input=sub_kwargs,
                capture_output=True,
                check=False,
                text=True,
                env=self.env,
            )
        if result.returncode != 0:
            msg = "\n".join(
                [
                    f"Wrapped: {cmd}",
                    f"{result.returncode = }",
                    "",
                    "STDOUT:",
                    result.stdout,
                    "",
                    "=" * 10,
                    "STDERR:",
                    result.stderr,
                ]
            )
            raise RuntimeError(msg)
        stdout = json.loads(result.stdout or "{}")
        return stdout, result.stderr


@LocalAdapter.register
class Hatch(WrappedRunner):
    @classmethod
    def funkify(cls, name, sub, env=None, path=None, hatch_path=None):
        if hatch_path is None:
            hatch_path = str(Path(shutil.which("hatch")).parent)
        script = [
            "#!/bin/bash",
            "set -e",
            "",
            f"export PATH={hatch_path}:~/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        ]
        if env is not None:
            for k, v in env.items():
                script.append(f"export {k}={v}\n")
        if path is not None:
            script.append(f"cd {shlex.quote(path)}")
        script.append(f"hatch -e {name} run $@")
        return WrappedRunner.funkify("\n".join(script), sub)


@LocalAdapter.register
class Conda(WrappedRunner):
    @classmethod
    def funkify(cls, name, sub, conda_loc="~/.local/conda", env=None):
        script = [
            "#!/bin/bash",
            "set -e",
            "",
            "export PATH=~/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
            f"source {conda_loc}/etc/profile.d/conda.sh",
            "",
        ]
        if env is not None:
            for k, v in env.items():
                script.append(f"export {k}={v}\n")
        script.append(f"conda activate {name}")
        script.append("$@")
        return WrappedRunner.funkify("\n".join(script), sub)


@LocalAdapter.register
class DockerRunner(Runner):
    _file_names = ("stdin.dump", "stdout.dump", "stderr.dump")

    @classmethod
    def funkify(cls, image, sub, flags=None):
        return {
            "sub": sub,
            "image": image,
            "flags": flags or [],
        }

    @staticmethod
    def start_docker(flags, image_uri, *sub_cmd):
        return _run_cli(["docker", "run", *flags, image_uri, *sub_cmd])

    @staticmethod
    def get_docker_status(cid):
        return _run_cli(["docker", "inspect", "-f", "{{.State.Status}}", cid], check=False) or "no-longer-exists"

    @staticmethod
    def get_docker_exit_code(cid):
        return int(_run_cli(["docker", "inspect", "-f", "{{.State.ExitCode}}", cid]))

    def submit(self):
        session = boto3.Session()
        creds = session.get_credentials()
        sub_uri, sub_kwargs, sub_adapter = self.sub_data()
        tmpd = mkdtemp(prefix="dml.")
        with open(f"{tmpd}/{self._file_names[0]}", "w") as f:
            f.write(sub_kwargs)
        env = self.env.copy()
        env.update(
            {
                "AWS_ACCESS_KEY_ID": creds.access_key,
                "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            }
        )
        if "DML_DEBUG" in os.environ:
            env["DML_DEBUG"] = os.environ["DML_DEBUG"]
        env_flags = [("-e", f"{k}={v}") for k, v in env.items()]
        flags = [
            "-v",
            f"{tmpd}:{tmpd}",
            "-d",
            *[y for x in env_flags for y in x],
            *self.kwargs.get("flags", []),
        ]
        container_id = self.start_docker(
            flags,
            self.kwargs["image"]["uri"],
            sub_adapter,
            "-n",
            "-1",
            "-i",
            f"{tmpd}/{self._file_names[0]}",
            "-o",
            f"{tmpd}/{self._file_names[1]}",
            "-e",
            f"{tmpd}/{self._file_names[2]}",
            sub_uri,
        )
        return container_id, tmpd

    def update(self, state):
        cid = state.get("cid")
        if cid is None:
            cid, tmpd = self.submit()
            return {"cid": cid, "tmpd": tmpd}, f"container {cid} started", {}
        tmpd = state["tmpd"]
        status = self.get_docker_status(cid)
        if status in ["created", "running"]:
            return state, f"container {cid} running", {}
        msg = f"container {cid} finished with status {status!r}"
        result = json.loads(if_read_file(f"{tmpd}/{self._file_names[1]}") or "{}")
        if result.get("dump"):
            return state, msg, result
        error_str = if_read_file(f"{tmpd}/{self._file_names[2]}") or ""
        exit_code = self.get_docker_exit_code(cid)
        msg = dedent(
            f"""
            Docker job {self.cache_key}
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
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            _run_cli(command, shell=True)
        super().gc(state)


@LocalAdapter.register
class SshRunner(Runner):
    @classmethod
    def funkify(cls, host, sub, port=None, user=None, keyfile=None, flags=None):
        return {
            "sub": sub,
            "host": host,
            "port": port,
            "user": user,
            "keyfile": keyfile,
            "flags": flags or [],
        }

    def _run_cmd(self, *user_cmd, **kw):
        flags = []
        if self.kwargs["keyfile"]:
            flags += ["-i", self.kwargs["keyfile"]]
        if self.kwargs["port"]:
            flags += ["-p", str(self.kwargs["port"])]
        flags = [*flags, *self.kwargs["flags"]]
        host = self.kwargs["host"]
        if self.kwargs["user"] is not None:
            host = self.kwargs["user"] + f"@{host}"
        cmd = ["ssh", *flags, host, " ".join(user_cmd)]
        resp = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=False,
            **kw,
        )
        if resp.returncode != 0:
            msg = f"Ssh {cmd}\n{resp.returncode = }\n{resp.stdout}\n\n{resp.stderr}"
            raise RuntimeError(msg)
        stderr = resp.stderr.strip()
        logger.debug(f"SSH STDERR: {stderr}")
        return resp.stdout.strip(), stderr

    def run(self):
        sub_uri, sub_kwargs, sub_adapter = self.sub_data()
        assert sub_adapter == LocalAdapter.ADAPTER
        runner = LocalAdapter.RUNNERS[sub_uri](**json.loads(sub_kwargs))
        script, sub_adapter, sub_uri, sub_kwargs = runner.get_script_and_args()
        tmpf, _ = self._run_cmd("mktemp", "-t", "dml.XXXXXX.sh")
        self._run_cmd("cat", ">", tmpf, input=script)
        self._run_cmd("chmod", "+x", tmpf)
        stdout, stderr = self._run_cmd(tmpf, sub_adapter, sub_uri, input=sub_kwargs)
        stdout = json.loads(stdout or "{}")
        self._run_cmd("rm", tmpf)
        return stdout, stderr


@LocalAdapter.register
class Cfn(Runner):
    @classmethod
    def funkify(cls, **data):
        return data

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
            raise RuntimeError("Cfn cannot create new stack while stack is currently being created")
        else:
            status = "creating"
        return state, self.fmt(state["StackId"], status, raw_status)

    def submit(self, client):
        assert Dml is not None, "dml is not installed..."
        with Dml() as dml:
            with dml.new(data=self.dump) as dag:
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
        client = boto3.client("cloudformation")
        result = {}
        if state == {}:
            state, msg = self.submit(client)
        else:
            state, msg = self.describe_stack(client, **state)
        if "outputs" in state:

            def _handler(dump):
                nonlocal result
                result["dump"] = dump

            try:
                with Dml() as dml:
                    with dml.new(data=self.dump, message_handler=_handler) as dag:
                        for k, v in state["outputs"].items():
                            dag[k] = v
                        dag.stack_id = state["StackId"]
                        dag.stack_name = state["name"]
                        dag.outputs = state["outputs"]
                        dag.result = dag.outputs
            except KeyboardInterrupt:
                raise
            except Exception:
                pass
            state.clear()
        return state, msg, result


@LocalAdapter.register
class Test(DockerRunner):
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

    def gc(self, state):
        if "cid" in state:
            _run_cli(["kill", "-9", str(state["cid"][0])], check=False)
        if "tmpd" in state:
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            _run_cli(command, shell=True)
        state["cid"] = "doesnotexist"
        super().gc(state)
