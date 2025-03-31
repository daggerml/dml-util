import json
import logging
import os
import shlex
import subprocess
import sys
from argparse import ArgumentParser
from contextlib import contextmanager
from dataclasses import dataclass
from tempfile import TemporaryDirectory
from textwrap import dedent
from time import sleep
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
from daggerml import Dml, Resource

from dml_util.baseutil import Runner, S3Store, WithDataError, get_client

logger = logging.getLogger(__name__)


def _read_data(file):
    if not isinstance(file, str):
        return file.read()
    if urlparse(file).scheme == "s3":
        return S3Store().get(file).decode()
    with open(file) as f:
        data = f.read()
        print(f"Filepath: {file} ------ {data = }", file=sys.stderr)
        return data.strip()


def _write_data(data, to):
    if not isinstance(to, str):
        return print(data, file=to)
    if urlparse(to).scheme == "s3":
        return S3Store().put(data.encode(), name=to)
    with open(to, "w") as f:
        f.write(data)


@dataclass
class Adapter:
    # TODO: add logs to dump
    ADAPTERS = {}

    @classmethod
    def cli(cls):
        parser = ArgumentParser()
        parser.add_argument("uri")
        parser.add_argument("-i", "--input", default=sys.stdin)
        parser.add_argument("-o", "--output", default=sys.stdout)
        parser.add_argument("-e", "--error", default=sys.stderr)
        parser.add_argument("-d", "--daemon", action="store_true")
        args = parser.parse_args()
        input = _read_data(args.input)
        while True:
            try:
                resp, msg = cls.send_to_remote(args.uri, input)
                _write_data(msg, args.error)
                if resp.get("dump"):
                    _write_data(json.dumps(resp), args.output)
                    sys.exit(0)
            except Exception as e:
                _write_data(str(WithDataError.from_ex(e)), args.error)  # to get the stacktrace
                sys.exit(1)
            if args.daemon:
                sleep(0.1)
            else:
                sys.exit(0)

    @classmethod
    def funkify(cls, uri, data):
        return Resource(uri, data=data, adapter=cls.ADAPTER)

    @classmethod
    def register(cls, def_cls):
        cls.ADAPTERS[def_cls.__name__.lower()] = def_cls
        return def_cls


@Adapter.register
class Lambda(Adapter):
    ADAPTER = "dml-util-lambda-adapter"

    @classmethod
    def send_to_remote(cls, uri, data):
        response = get_client("lambda").invoke(
            FunctionName=uri,
            InvocationType="RequestResponse",
            LogType="Tail",
            Payload=data.strip().encode(),
        )
        payload = json.loads(response["Payload"].read())
        if payload.get("status", 400) // 100 in [4, 5]:
            raise WithDataError(payload.get("status", payload))
        out = payload.get("response", {})
        return out, payload.get("message")


@Adapter.register
class Local(Adapter):
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
        cls.RUNNERS[def_cls.__name__.lower()] = def_cls
        return def_cls

    @classmethod
    def send_to_remote(cls, uri, data):
        runner = cls.RUNNERS[uri](**json.loads(data))
        return runner.run()


def _run_cli(command, **kw):
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        check=False,
        **kw,
    )
    if result.returncode != 0:
        msg = f"{command}\n{result.returncode = }\n{result.stdout}\n\n{result.stderr}"
        raise RuntimeError(msg)
    return result.stdout.strip()


@Local.register
class Script(Runner):
    @classmethod
    def funkify(cls, script, cmd=("python3",)):
        return {"script": script, "cmd": list(cmd)}

    def to_cmd(self, filepath):
        return [*self.kwargs["cmd"], filepath]

    def submit(self):
        tmpd = _run_cli("mktemp -d -t dml.XXXXXX".split())
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
            self.to_cmd(f"{tmpd}/script"),
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
            return {"pid": pid, "tmpd": tmpd}, f"{pid = } started", {}

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
            return state, f"{pid = } running", {}
        if os.path.isfile(f"{tmpd}/output.dump"):
            with open(f"{tmpd}/output.dump") as f:
                dump = f.read()
            s3 = S3Store()
            resp = {"dump": dump}
            try:  # FIXME: logging is kinda messed up... fix it.
                logs = {k: f"{tmpd}/{k}" for k in ["stdout", "stderr"]}
                logs = {k: s3.put(filepath=v, suffix=".log").uri for k, v in logs.items() if os.path.isfile(v)}
                resp["logs"] = logs
            except Exception:
                pass
            return state, f"{pid = } finished", resp
        msg = f"{pid = } finished without writing output"
        if os.path.exists(f"{tmpd}/stderr"):
            with open(f"{tmpd}/stderr", "r") as f:
                msg = f"{msg}\nSTDERR:\n-------\n{f.read()}"
        raise RuntimeError(msg)

    def delete(self, state):
        if "pid" in state:
            _run_cli(f"kill -9 {state['pid']} || echo", shell=True)
        if "tmpd" in state:
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            _run_cli(command, shell=True)
        super().delete(state)


@Local.register
class Wrapped(Runner):
    @classmethod
    def funkify(cls, script, sub):
        kw = {"script": script, "sub": sub}
        return kw

    def run(self):
        sub_uri, sub_kwargs, sub_adapter = self._to_data()
        with TemporaryDirectory() as tmpd:
            with open(f"{tmpd}/script", "w") as f:
                f.write(self.kwargs["script"])
            subprocess.run(["chmod", "+x", f"{tmpd}/script"], check=True)
            cmd = [f"{tmpd}/script", sub_adapter, sub_uri]
            result = subprocess.run(
                cmd,
                input=sub_kwargs,
                capture_output=True,
                check=False,
                text=True,
            )
        if result.returncode != 0:
            msg = "\n".join(
                [
                    str(cmd),
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


@Local.register
class Hatch(Wrapped):
    @classmethod
    def funkify(cls, name, sub, path=None):
        script = "#!/bin/bash\n\n"
        if path is not None:
            script += f"cd {shlex.quote(path)}\n"
        script += f"hatch -e {name} run $1 $2"
        return super().funkify(script, sub)


@Local.register
class Docker(Runner):
    @classmethod
    def funkify(cls, image, sub, flags=None):
        return {
            "sub": sub,
            "image": image,
            "flags": flags or [],
        }

    def submit(self):
        session = boto3.Session()
        creds = session.get_credentials()
        sub_uri, sub_kwargs, sub_adapter = self._to_data()
        tmpd = _run_cli("mktemp -d -t dml.XXXXXX".split())
        with open(f"{tmpd}/stdin.dump", "w") as f:
            f.write(sub_kwargs)
        env = {
            "AWS_ACCESS_KEY_ID": creds.access_key,
            "AWS_SECRET_ACCESS_KEY": creds.secret_key,
            **self.env,
        }
        env_flags = [("-e", f"{k}={v}") for k, v in env.items()]
        container_id = _run_cli(
            [
                "docker",
                "run",
                "-v",
                f"{tmpd}:/opt/dml",
                *[y for x in env_flags for y in x],
                "-d",  # detached
                # "-i",
                *self.kwargs.get("flags", []),
                "--entrypoint",
                sub_adapter,
                self.kwargs["image"]["uri"],
                # "-h",
                "-d",
                "-i",
                "/opt/dml/stdin.dump",
                "-o",
                "/opt/dml/stdout.dump",
                "-e",
                "/opt/dml/stderr.dump",
                sub_uri,
            ],
            # input=sub_kwargs,
        )
        return container_id, tmpd

    def update(self, state):
        cid = state.get("cid")
        if cid is None:
            cid, tmpd = self.submit()
            return {"cid": cid, "tmpd": tmpd}, f"container {cid} started", {}
        tmpd = state["tmpd"]
        try:
            status = _run_cli(["docker", "inspect", "-f", "{{.State.Status}}", cid])
        except KeyboardInterrupt:
            raise
        except Exception:
            status = "no-longer-exists"
        if status in ["created", "running"]:
            return state, f"container {cid} running", {}
        # response["logs"] = {"combined": S3Store().put(_run_cli(["docker", "logs", cid]).encode(), suffix=".log").uri}
        # raise ValueError("ahhhhhhhh %r" % (state,))
        sleep(0.1)
        msg = f"container {cid} finished with status {status!r}"
        error_str = ""
        if not os.path.exists(tmpd):
            msg += "   --- TMPD DOES NOT EXIST!!!"
        if os.path.exists(f"{tmpd}/stderr.dump"):
            with open(f"{tmpd}/stderr.dump") as f:
                error_str = f.read()
        result = {}
        if os.path.exists(f"{tmpd}/stdout.dump"):
            with open(f"{tmpd}/stdout.dump") as f:
                result = json.loads(f.read() or '{"wtf":23}')
        if status in ["exited"] and result:
            return state, msg, result
        # raise RuntimeError(msg)
        dkr_logs = _run_cli(["docker", "logs", cid])
        exit_code = int(_run_cli(["docker", "inspect", "-f", "{{.State.ExitCode}}", cid]))
        msg = dedent(
            f"""
            job {self.cache_key}
              {msg}
              exit code {exit_code}
              No output written
              Logs:
                {dkr_logs}
              STDERR:
                {error_str}
            ================
            """
        ).strip()
        raise RuntimeError(msg)

    def delete(self, state):
        if "cid" in state:
            _run_cli(["docker", "rm", state["cid"]])
        if "tmpd" in state:
            command = "rm -r {} || echo".format(shlex.quote(state["tmpd"]))
            _run_cli(command, shell=True)
        super().delete(state)


@Local.register
class Docker_(Runner):
    @classmethod
    def funkify(cls, **data):
        return data

    def _run_command(self, command):
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=False)
            return result.returncode, (result.stdout + result.stderr).strip()
        except subprocess.SubprocessError as e:
            return 1, str(e)

    def submit(self):
        tmpd = _run_cli("mktemp -d -t dml.XXXXXX".split())
        with open(f"{tmpd}/script", "w") as f:
            f.write(self.kwargs["script"])
        subprocess.run(["chmod", "+x", f"{tmpd}/script"], check=True)
        with open(f"{tmpd}/input.dump", "w") as f:
            f.write(self.dump)
        env_flags = [("-e", f"{k}={v}") for k, v in self.env.items()]
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
                self.kwargs["image"]["uri"],
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
                s3 = S3Store()
                _, stdout = self._run_command(["docker", "logs", cid])
                with open(f"{tmpd}/output.dump") as f:
                    return {
                        "logs": {"stdout/stderr": s3.put(stdout.encode(), suffix=".log").uri},
                        "dump": f.read(),
                    }
            _, exit_code_str = self._run_command(["docker", "inspect", "-f", "{{.State.ExitCode}}", cid])
            _, logs = self._run_command(["docker", "logs", cid])
            exit_code = int(exit_code_str)
            msg = f"""
            job {self.cache_key}
              finished with status {status}
              exit code {exit_code}
              No output written
              Logs:
                {logs}
            """.strip()
            raise RuntimeError(msg)
        finally:
            if os.getenv("DML_DOCKER_CLEANUP") == "1":
                self._run_command(["docker", "rm", cid])

    def update(self, state):
        cid = state.get("cid")
        response = {}
        if cid is None:
            cid, tmpd = self.submit()
            return {"cid": cid, "tmpd": tmpd}, f"container {cid} started", response
        # Check if container exists and get its status
        tmpd = state["tmpd"]
        exit_code, status = self._run_command(["docker", "inspect", "-f", "{{.State.Status}}", cid])
        status = status if exit_code == 0 else "no-longer-exists"
        if status in ["created", "running", "restarting"]:
            return state, f"container {cid} running", response
        elif status in ["exited", "paused", "dead", "no-longer-exists"]:
            msg = f"container {cid} finished with status {status!r}"
            response = self.maybe_complete(tmpd, cid, status)
            return state, msg, response


@Local.register
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
            raise RuntimeError("cannot create new stack while stack is currently being created")
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


@Local.register
class Test(Runner):
    @classmethod
    def funkify(cls, **kw):
        return kw

    def run(self):
        outdump = None

        def handler(dump):
            nonlocal outdump
            outdump = dump

        with Dml() as dml:
            with dml.new(data=self.dump, message_handler=handler) as dag:
                dag.result = dag.argv
        return {"dump": outdump}, "finished local testing stuff?!"


@contextmanager
def aws_fndag():
    import os
    from urllib.parse import urlparse

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

    with Dml() as dml:
        with dml.new(data=_get_data(), message_handler=_handler) as dag:
            yield dag
