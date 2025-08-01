"""Tests for the Runner classes and implementations."""

import os
import subprocess
from shutil import which
from unittest.mock import MagicMock, patch

import pytest

from dml_util.core.config import EnvConfig, InputConfig
from dml_util.runners import (
    CondaRunner,
    DockerRunner,
    HatchRunner,
    ScriptRunner,
    UvRunner,
    WrappedRunner,
)


@pytest.mark.usefixtures("adapter_setup")
class TestScriptRunner:
    """Tests specific to ScriptRunner."""

    @pytest.fixture
    def script_runner(self):
        """Return a ScriptRunner instance."""
        input_config = InputConfig(
            cache_path="/tmp/cache",
            cache_key="test-key",
            kwargs={
                "script": "print('Hello, World!')",
                "cmd": ["python3"],
                "suffix": ".py",
            },
            dump="{}",
        )
        with patch("dml_util.runners.local.ScriptRunner.state_class"):
            runner = ScriptRunner(EnvConfig.from_env(), input_config)
            return runner

    def test_script_runner_funkify(self):
        """Test ScriptRunner.funkify method."""
        script = "print('Hello, World!')"
        cmd = ["python3"]
        suffix = ".py"
        result = ScriptRunner.funkify(script, cmd=cmd, suffix=suffix)
        assert result["script"] == script
        assert result["cmd"] == cmd
        assert result["suffix"] == suffix

    def test_script_runner_submit(self, script_runner):
        """Test ScriptRunner.submit method."""
        with patch("dml_util.runners.local.mkdtemp", return_value="/tmp/dml.123456"):
            with patch("dml_util.runners.local.open", MagicMock()):
                with patch("dml_util.runners.local.launch_detached", return_value=12345):
                    proc_id, tmpd = script_runner.submit()
                    assert proc_id == 12345
                    assert tmpd == "/tmp/dml.123456"

    def test_script_runner_update_new_process(self, script_runner):
        """Test ScriptRunner.update method for new process."""
        with patch.object(script_runner, "submit", return_value=(12345, "/tmp/dml.123456")):
            new_state, msg, response = script_runner.update({})
            assert new_state == {"pid": 12345, "tmpd": "/tmp/dml.123456"}
            assert "started" in msg
            assert response is None

    def test_script_runner_update_running_process(self, script_runner):
        """Test ScriptRunner.update method for running process."""
        state = {"pid": 12345, "tmpd": "/tmp/dml.123456"}

        with patch("dml_util.runners.local.proc_exists", return_value=True):
            new_state, msg, response = script_runner.update(state)
            assert new_state == state
            assert "running" in msg
            assert response is None

    def test_script_runner_update_finished_process(self, script_runner):
        """Test ScriptRunner.update method for finished process."""
        state = {"pid": 12345, "tmpd": "/tmp/dml.123456"}
        with patch("dml_util.runners.local.proc_exists", return_value=False):
            with patch("dml_util.runners.local.if_read_file", return_value="test output"):
                new_state, msg, response = script_runner.update(state)
                assert new_state is None
                assert "finished" in msg
                assert response == "test output"

    def test_script_runner_update_finished_no_output(self, script_runner):
        """Test ScriptRunner.update method for finished process without output."""
        state = {"pid": 12345, "tmpd": "/tmp/dml.123456"}
        with patch("dml_util.runners.local.proc_exists", return_value=False):
            with patch("dml_util.runners.local.if_read_file", return_value=None):
                with pytest.raises(RuntimeError) as excinfo:
                    script_runner.update(state)
                assert "finished without writing output" in str(excinfo.value)


@pytest.mark.usefixtures("adapter_setup")
class TestWrappedRunner:
    """Tests specific to WrappedRunner."""

    @pytest.fixture
    def wrapped_runner(self):
        """Return a WrappedRunner instance."""
        input_config = InputConfig(
            cache_path="/tmp/cache",
            cache_key="test-key",
            kwargs={
                "script": "#!/bin/bash\necho 'Hello, World!'",
                "sub": {
                    "adapter": "test-adapter",
                    "uri": "test-uri",
                    "data": {"param": "value"},
                },
            },
            dump="{}",
        )
        runner = WrappedRunner(EnvConfig.from_env(), input_config)
        return runner

    def test_wrapped_runner_funkify(self):
        """Test WrappedRunner.funkify method."""
        script = "#!/bin/bash\necho 'Hello, World!'"
        sub = {"adapter": "test-adapter", "uri": "test-uri", "data": {"param": "value"}}

        result = WrappedRunner.funkify(script, sub)

        assert result["script"] == script
        assert result["sub"] == sub

    def test_wrapped_runner_run_success(self, wrapped_runner):
        """Test WrappedRunner.run method with successful execution."""
        mock_process = MagicMock()
        mock_process.returncode = 0
        mock_process.stdout = "test output"
        mock_process.stderr = "test log"
        with patch("subprocess.run", return_value=mock_process):
            output, log = wrapped_runner.run()
            assert output == "test output"
            assert log == "test log"

    def test_wrapped_runner_run_error(self, wrapped_runner):
        """Test WrappedRunner.run method with error."""
        mock_process = MagicMock()
        mock_process.returncode = 1
        mock_process.stdout = "error output"
        mock_process.stderr = "error log"
        with patch("subprocess.run", return_value=mock_process):
            with pytest.raises(RuntimeError) as excinfo:
                wrapped_runner.run()
            assert "error output" in str(excinfo.value)
            assert "error log" in str(excinfo.value)


@pytest.mark.usefixtures("adapter_setup")
class TestHatchRunner:
    """Tests specific to HatchRunner."""

    def test_hatch_runner_funkify(self):
        """Test HatchRunner.funkify method."""
        with patch("dml_util.runners.local.WrappedRunner.funkify") as mock_funkify:
            HatchRunner.funkify(
                name="test-env",
                sub={
                    "adapter": "test-adapter",
                    "uri": "test-uri",
                    "data": {"param": "value"},
                },
                path="/path/to/project",
            )

            # Verify WrappedRunner.funkify was called with a script and sub
            args, kwargs = mock_funkify.call_args
            script, sub = args

            assert "hatch env create test-env" in script
            assert "cd /path/to/project" in script
            assert sub == {
                "adapter": "test-adapter",
                "uri": "test-uri",
                "data": {"param": "value"},
            }

    @pytest.mark.skipif(not which("hatch"), reason="hatch command not found")
    def test_hatch_script_passes_env(self):
        js = HatchRunner.funkify("pandas", None)
        resp = subprocess.run(
            ["bash", "-c", js["script"], "_", "env"],
            env={"DML_CACHE_KEY": "test_key", "DML_CACHE_PATH": "foo", "PATH": os.environ["PATH"]},
            input="testing...",
            capture_output=True,
            timeout=1,
            text=True,
        )
        assert resp.returncode == 0, f"Script failed: {resp.stderr}"
        lines = resp.stdout.splitlines()
        env = {k: v for k, v in (x.split("=", 1) for x in lines) if k.startswith("DML_")}
        assert env["DML_CACHE_KEY"] == "test_key"
        assert env["DML_CACHE_PATH"] == "foo"


@pytest.mark.usefixtures("adapter_setup")
class TestCondaRunner:
    """Tests specific to CondaRunner."""

    def test_conda_runner_funkify(self):
        """Test CondaRunner.funkify method."""
        with patch("dml_util.runners.local._run_cli", return_value="/usr/local/conda"):
            with patch("dml_util.runners.local.WrappedRunner.funkify") as mock_funkify:
                CondaRunner.funkify(
                    name="test-env",
                    sub={
                        "adapter": "test-adapter",
                        "uri": "test-uri",
                        "data": {"param": "value"},
                    },
                )
                # Verify WrappedRunner.funkify was called with a script and sub
                args, kwargs = mock_funkify.call_args
                script, sub = args
                assert "source /usr/local/conda/etc/profile.d/conda.sh" in script
                assert "conda activate test-env" in script
                assert sub == {
                    "adapter": "test-adapter",
                    "uri": "test-uri",
                    "data": {"param": "value"},
                }

    @pytest.mark.skipif(not which("conda"), reason="conda command not found")
    def test_conda_script_passes_env(self):
        js = CondaRunner.funkify("dml-pandas", None)
        resp = subprocess.run(
            ["bash", "-c", js["script"], "script", "env"],
            env={"DML_CACHE_KEY": "test_key", "DML_CACHE_PATH": "foo"},
            input="testing...",
            capture_output=True,
            timeout=1,
            text=True,
        )
        assert "DML_CACHE_KEY=test_key" in resp.stdout
        assert "DML_CACHE_PATH=foo" in resp.stdout


@pytest.mark.skipif(not which("uv"), reason="uv command not found")
def test_uvrunner_cli(tmp_path):
    subprocess.run(["uv", "init", "--bare"], check=True, cwd=tmp_path)
    script_path = tmp_path / "script.sh"
    script_path.write_text(UvRunner.funkify(None, str(tmp_path))["script"])
    subprocess.run(["chmod", "+x", str(script_path)], check=True)
    pycmd = "import sys; print(sys.executable)"
    resp = subprocess.run([str(script_path), "python", "-c", pycmd], check=True, capture_output=True, text=True)
    assert resp.stdout.strip().startswith(str(tmp_path))  # we're using the right python


class TestDockerRunner:
    @pytest.mark.usefixtures("adapter_setup")
    def test_calls(self):
        data = {
            "cache_key": "foo:key",
            "cache_path": "bar",
            "kwargs": {
                "sub": {"uri": "bar", "data": {}, "adapter": "baz"},
                "image": {"uri": "foo:uri"},
            },
            "dump": "opaque",
        }
        dkr = DockerRunner(EnvConfig.from_env(), InputConfig(**data))
        # patch mkdtemp to return env[FN_CACHE_DIR
        with patch(
            "dml_util.runners.container.mkdtemp",
            return_value=os.getenv("DML_FN_CACHE_DIR"),
        ):
            with patch.object(DockerRunner, "start_docker", return_value="testing0"):
                resp = dkr.update({})  # no state
                assert resp[1:] == ("container testing0 started", None)
            assert resp[0]["tmpd"] == os.getenv("DML_FN_CACHE_DIR")
        with patch("dml_util.runners.container._run_cli") as mock_run_cli:
            # running job
            mock_run_cli.side_effect = ["running", "_logs_"]
            resp = dkr.update({"cid": "foo123"})
            assert resp[1:] == ("container foo123 running -- _logs_", None)

            # patch
            with patch("dml_util.runners.container.if_read_file", return_value="opaque"):
                mock_run_cli.side_effect = ["exited", "cool-logs", "0"]
                resp = dkr.update({"cid": "foo123", "tmpd": "/does/not/exist"})
                assert resp[1:] == (
                    "container foo123 finished with status 'exited'",
                    "opaque",
                )
            # bad exit
            with patch("dml_util.runners.container.if_read_file", side_effect=["", "stderr"]):
                mock_run_cli.side_effect = ["exited", "cool-logs", "0"]
                with pytest.raises(RuntimeError):
                    dkr.update({"cid": "foo123", "tmpd": "/does/not/exist"})
