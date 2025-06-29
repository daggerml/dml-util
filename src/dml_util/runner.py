import json
import logging
import os
import shlex
import shutil
import subprocess
from pathlib import Path
from tempfile import TemporaryDirectory, mkdtemp
from textwrap import dedent

import boto3
from botocore.exceptions import ClientError
from daggerml import Dml

from dml_util.adapter import LocalAdapter
from dml_util.baseutil import Runner, _run_cli, if_read_file, proc_exists
from dml_util.lib.submit import launch_detached

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


@LocalAdapter.register
class ScriptRunner(Runner):
    @classmethod
    def funkify(cls, script, cmd=("python3",), suffix=".py"):
        return {"script": script, "cmd": list(cmd), "suffix": suffix}

    def submit(self):
        logger.debug("Submitting script to local runner")
        tmpd = mkdtemp(prefix="dml.")
        script_path = f"{tmpd}/script" + (self.kwargs["suffix"] or "")
        with open(script_path, "w") as f:
            f.write(self.kwargs["script"])
        with open(f"{tmpd}/input.dump", "w") as f:
            f.write(self.dump)
        env = {
            **self.env,
            "DML_INPUT_LOC": f"{tmpd}/input.dump",
            "DML_OUTPUT_LOC": f"{tmpd}/output.dump",
            "DML_S3_PREFIX": f"{self.s3_prefix}/data",
        }
        logger.debug(f"Environment for script: {json.dumps(env)}")
        proc_id = launch_detached([*self.kwargs["cmd"], script_path], env=env)
        return proc_id, tmpd

    def update(self, state):
        # TODO: update logging to include message
        # TODO: remove stderr printing unless debug or error
        pid = state.get("pid")
        response = None
        if pid is None:
            pid, tmpd = self.submit()
            logger.info(f"Process {pid} started in {tmpd}")
            return {"pid": pid, "tmpd": tmpd}, f"{pid = } started", response
        tmpd = state["tmpd"]
        if proc_exists(pid):
            logger.debug(f"Process {pid} is still running")
            return state, f"{pid = } running", response
        logger.info(f"Process {pid} finished, checking output")
        dump = if_read_file(f"{tmpd}/output.dump")
        if dump:
            logger.debug(f"Process {pid} wrote output. Returning.")
            return None, f"{pid = } finished", dump
        logger.warning(f"Process {pid} did not write output, raising error")
        msg = f"[Script] {pid = } finished without writing output"
        raise RuntimeError(msg)

    def gc(self, state):
        logger.debug(f"Cleaning up state: {state}")
        if "pid" in state:
            logger.debug(f"Killing process {state['pid']}")
            _run_cli(f"kill -9 {state['pid']} || echo", shell=True)
        if "tmpd" in state:
            logger.debug(f"Removing temporary directory {state['tmpd']}")
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            _run_cli(command, shell=True)
        logger.debug("Calling super().gc()")
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
            env = os.environ.copy()
            env.update(self.env)
            result = subprocess.run(
                cmd,
                input=sub_kwargs,
                capture_output=True,
                check=False,
                text=True,
                env=env,
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
        return result.stdout, result.stderr


@LocalAdapter.register
class HatchRunner(WrappedRunner):
    @classmethod
    def funkify(cls, name, sub, path=None, hatch_path=None):
        if hatch_path is None:
            hatch_path = str(Path(shutil.which("hatch")).parent)
            logger.info("Set hatch path to: %r", hatch_path)
        cd_str = "" if path is None else f"cd {shlex.quote(path)}"
        script = dedent(
            f"""
            #!/usr/bin/env bash
            set -euo pipefail

            export PATH={shlex.quote(hatch_path)}:$PATH
            which hatch >&2 || {{ echo "ERROR: hatch not found in PATH" >&2; exit 1; }}
            {cd_str}
            hatch env create {name} >&2 || {{ echo "ERROR: hatch env create failed" >&2; exit 1; }}

            INPUT_DATA=$(cat)
            # if DML_DEBUG is set, print input data to stderr
            if [[ -n "${{DML_DEBUG:-}}" ]]; then
                echo "INPUT DATA:" >&2
                echo "$INPUT_DATA" >&2
                echo "DONE with input data" >&2
            fi
            echo "$INPUT_DATA" | {shlex.quote(hatch_path)}/hatch -e {name} run "$@"
            """
        ).strip()
        return WrappedRunner.funkify(script, sub)


@LocalAdapter.register
class CondaRunner(WrappedRunner):
    @classmethod
    def funkify(cls, name, sub, conda_loc=None):
        if conda_loc is None:
            conda_loc = str(_run_cli(["conda", "info", "--base"]).strip())
            logger.info("Using conda from %r", conda_loc)
        script = dedent(
            f"""
            #!/usr/bin/env bash
            set -euo pipefail

            source {shlex.quote(conda_loc)}/etc/profile.d/conda.sh
            conda deactivate || echo 'no active conda environment to deactivate' >&2
            conda activate {name} >&2
            exec "$@"
            """
        ).strip()
        return WrappedRunner.funkify(script, sub)


@LocalAdapter.register
class DockerRunner(Runner):
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
        dkr = self.kwargs.get("docker_path") or "docker"
        return _run_cli([dkr, *args], **kwargs)

    def start_docker(self, flags, image_uri, *sub_cmd):
        envs = [("-e", f"{k}={v}") for k, v in self.env.items() if k.startswith("DML_")]
        envs = [x for y in envs for x in y]
        return self._dkr("run", *flags, *envs, image_uri, *sub_cmd)

    def get_docker_status(self, cid):
        return self._dkr("inspect", "-f", "{{.State.Status}}", cid, check=False) or "no-longer-exists"

    def get_docker_exit_code(self, cid):
        return int(self._dkr("inspect", "-f", "{{.State.ExitCode}}", cid))

    def get_docker_logs(self, cid):
        return self._dkr("logs", cid, check=False)

    def submit(self):
        sub_uri, sub_kwargs, sub_adapter = self.sub_data()
        tmpd = mkdtemp(prefix="dml.")
        with open(f"{tmpd}/{self._file_names[0]}", "w") as f:
            f.write(sub_kwargs)
        env_flags = [("-e", f"{k}={v}") for k, v in self.env.items()]
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
            "--debug",
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
            return {"cid": cid, "tmpd": tmpd}, f"container {cid} started", None
        tmpd = state["tmpd"]
        status = self.get_docker_status(cid)
        dkr_logs = self.get_docker_logs(cid)
        if status in ["created", "running"]:
            return state, f"container {cid} running -- {dkr_logs}", None
        msg = f"container {cid} finished with status {status!r}"
        result = if_read_file(f"{tmpd}/{self._file_names[1]}")
        if result:
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
            _run_cli(["rm", "-r", state["tmpd"]], check=False)
        super().gc(state)


@LocalAdapter.register
class SshRunner(Runner):
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
        shbang, *lines = self.kwargs["script"].split("\n")
        env_lines = [f"export {k}={shlex.quote(v)}" for k, v in self.env.items() if k.startswith("DML_")]
        script = "\n".join([shbang, *env_lines, *lines])
        self._run_cmd("cat", ">", tmpf, input=script)
        self._run_cmd("chmod", "+x", tmpf)
        return tmpf

    def _run_cmd(self, *user_cmd, **kw):
        cmd = ["ssh", *self.kwargs["flags"], self.kwargs["host"], *user_cmd]
        resp = subprocess.run(cmd, capture_output=True, text=True, check=False, **kw)
        if resp.returncode != 0:
            msg = f"Ssh(code:{resp.returncode}) {user_cmd}\nSTDOUT\n{resp.stdout}\n\nSTDERR\n{resp.stderr}"
            raise RuntimeError(msg)
        stderr = resp.stderr.strip()
        logger.debug(f"SSH STDERR: {stderr}")
        return resp.stdout.strip(), stderr

    def run(self):
        sub_uri, sub_kwargs, sub_adapter = self.sub_data()
        tmpf = self.proc_script()
        stdout, stderr = self._run_cmd(tmpf, sub_adapter, sub_uri, input=sub_kwargs)
        # stdout = json.loads(stdout or "{}")
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
        with Dml.temporary() as dml:
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
                with Dml.temporary() as dml:
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
        return state, msg, result.get("dump")


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
