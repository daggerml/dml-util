import getpass
import os
import shlex
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path
from tempfile import TemporaryDirectory
from textwrap import dedent

import pytest


@pytest.fixture
def sshd_server():
    """Fixture to start and stop the SSH server for testing."""
    try:
        with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
            sock.close()
            host_key_path = os.path.join(tmpd, "ssh_host_rsa_key")
            subprocess.run(
                ["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-f", host_key_path],
                check=True,
            )
            client_key_path = os.path.join(tmpd, "client_key")
            subprocess.run(
                ["ssh-keygen", "-q", "-t", "rsa", "-N", "", "-f", client_key_path],
                check=True,
            )
            authorized_keys_path = os.path.join(tmpd, "authorized_keys")
            client_pub_key_path = client_key_path + ".pub"
            shutil.copy(client_pub_key_path, authorized_keys_path)
            os.chmod(authorized_keys_path, 0o600)
            sshd_config_path = os.path.join(tmpd, "sshd_config")
            pid_file = os.path.join(tmpd, "sshd.pid")
            with open(sshd_config_path, "w") as f:
                f.write(
                    dedent(
                        f"""
                        Port {port}
                        ListenAddress 127.0.0.1
                        HostKey {host_key_path}
                        PidFile {pid_file}
                        LogLevel DEBUG
                        UsePrivilegeSeparation no
                        StrictModes no
                        PasswordAuthentication no
                        ChallengeResponseAuthentication no
                        PubkeyAuthentication yes
                        AuthorizedKeysFile {authorized_keys_path}
                        UsePAM no
                        Subsystem sftp internal-sftp
                        """
                    ).strip()
                )
            sshd_path = shutil.which("sshd")
            assert sshd_path, "sshd must be available in PATH"
            sshd_proc = subprocess.Popen(
                [sshd_path, "-f", sshd_config_path, "-D"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            flags = [
                "-i",
                client_key_path,
                "-p",
                str(port),
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
            ]

            deadline = time.time() + 5  # wait up to 5 seconds
            while time.time() < deadline:
                if sshd_proc.poll() is not None:
                    stdout, stderr = sshd_proc.communicate(timeout=1)
                    raise RuntimeError(
                        f"sshd terminated unexpectedly.\nstdout: {stdout.decode()}\nstderr: {stderr.decode()}"
                    )
                try:
                    test_sock = socket.create_connection(("127.0.0.1", port), timeout=0.5)
                    test_sock.close()
                    break
                except (ConnectionRefusedError, OSError):
                    time.sleep(0.1)
            else:
                raise RuntimeError("Timeout waiting for sshd to start.")
            yield flags, f"{getpass.getuser()}@127.0.0.1"
    finally:
        if sshd_proc:
            # print stdout and stderr from sshd
            stdout, stderr = sshd_proc.communicate(timeout=1)
            if stdout:
                print(f"sshd stdout:\n{stdout.decode()}", file=sys.stderr)
            if stderr:
                print(f"sshd stderr:\n{stderr.decode()}", file=sys.stderr)
            sshd_proc.terminate()
            try:
                sshd_proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                sshd_proc.kill()


@pytest.fixture
def ssh_resource_data(sshd_server, tmpdir, aws_server):
    env_file = os.path.join(tmpdir, "env_file")
    with TemporaryDirectory(prefix="dml-util-test-") as tmpd:
        with open(env_file, "w") as f:
            f.write(
                dedent(
                    f"""
                    export DML_FN_CACHE_DIR={tmpd}
                    export PATH={shlex.quote(str(Path(sys.executable).parent))}:$PATH
                    """
                ).strip()
            )
            for x in [shutil.which(y) for y in ["docker", "hatch", "uv"]]:
                if x:
                    f.write(f"\nexport PATH={shlex.quote(os.path.dirname(x))}:$PATH")
            for k, v in aws_server["envvars"].items():
                f.write(f"\nexport {k}={v}")
        resource_data = {
            "host": sshd_server[1],
            "flags": sshd_server[0],
            "env_files": [env_file],
        }
        yield resource_data


@pytest.fixture
def docker_flags(aws_server, tmpdir):
    aws_env = aws_server["envvars"]
    with open(f"{tmpdir}/credentials", "w") as f:
        f.write("[default]\n")
        f.write(f"aws_access_key_id={aws_env['AWS_ACCESS_KEY_ID']}\n")
        f.write(f"aws_secret_access_key={aws_env['AWS_SECRET_ACCESS_KEY']}\n")
    with open(f"{tmpdir}/config", "w") as f:
        f.write("[default]\n")
        f.write(f"region={aws_env['AWS_DEFAULT_REGION']}\n")
    flags = [
        "--platform",
        "linux/amd64",
        "--add-host=host.docker.internal:host-gateway",
        "-e",
        f"AWS_ENDPOINT_URL=http://host.docker.internal:{aws_server['port']}",
        "-e",
        "AWS_SHARED_CREDENTIALS_FILE=/root/.aws/credentials",
        "-v",
        f"{shlex.quote(str(tmpdir))}/credentials:/root/.aws/credentials:ro",
        "-v",
        f"{shlex.quote(str(tmpdir))}/config:/root/.aws/config:ro",
    ]
    return flags
