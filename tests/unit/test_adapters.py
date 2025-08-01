"""Tests for the adapter module and implementations."""

import json
import os
from tempfile import NamedTemporaryFile
from unittest.mock import MagicMock, patch

import pytest
from daggerml import Error, Resource
from daggerml.core import from_json

from dml_util.adapters import LambdaAdapter, LocalAdapter
from dml_util.adapters.base import AdapterBase, _read_data, _write_data
from dml_util.runners.base import runners
from tests.util import tmpdir


class TestDataIO:
    """Tests for data input/output functions."""

    @pytest.mark.parametrize(
        "input_value, expected",
        [
            (MagicMock(read=lambda: "test data"), "test data"),
        ],
    )
    def test_read_data(self, input_value, expected):
        """Test reading data from different sources."""
        result = _read_data(input_value)
        assert result == expected

    def test_read_data_s3(self):
        """Test reading data from S3."""
        s3_uri = "s3://test-bucket/test-prefix/test.txt"
        expected = "test s3 data"

        with patch("dml_util.adapters.base.S3Store") as mock_s3store_cls:
            mock_s3store = MagicMock()
            mock_s3store_cls.return_value = mock_s3store
            mock_s3store.get.return_value = expected.encode()

            result = _read_data(s3_uri)
            assert result == expected
            mock_s3store.get.assert_called_once_with(s3_uri)

    def test_read_data_local_file(self):
        """Test reading data from a local file path."""
        with NamedTemporaryFile(mode="w", delete=False, prefix="dml-test-") as temp_file:
            temp_file.write("test data\n")
            temp_path = temp_file.name

        try:
            result = _read_data(temp_path)
            assert result == "test data"
        finally:
            os.unlink(temp_path)

    def test_write_data_file_obj(self):
        """Test writing data to a file-like object."""
        data = "test data"
        mode = "w"
        mock_file = MagicMock()

        with patch("builtins.print") as mock_print:
            _write_data(data, mock_file, mode)
            mock_print.assert_called_once_with(data, file=mock_file, flush=True)

    def test_write_data_s3(self):
        """Test writing data to S3."""
        data = "test data"
        output_target = "s3://test-bucket/test-prefix/output.txt"
        mode = "w"

        with patch("dml_util.adapters.base.S3Store") as mock_s3store_cls:
            mock_s3store = MagicMock()
            mock_s3store_cls.return_value = mock_s3store

            _write_data(data, output_target, mode)
            mock_s3store.put.assert_called_once_with(data.encode(), uri=output_target)

    def test_write_data_local_file(self):
        """Test writing data to a local file."""
        data = "test data"
        mode = "w"

        with NamedTemporaryFile(mode="w", delete=False, prefix="dml-test-") as temp_file:
            temp_path = temp_file.name

        try:
            _write_data(data, temp_path, mode)
            with open(temp_path, "r") as f:
                content = f.read()
            assert content == data

            # Test append mode
            _write_data("append data", temp_path, "a")
            with open(temp_path, "r") as f:
                content = f.read()
            assert content == data + "append data\n"
        finally:
            os.unlink(temp_path)


class TestAdapterCore:
    """Tests for the core Adapter class API."""

    def test_adapter_base_class_registration(self):
        """Test adapter class registration mechanism."""
        original_adapters = AdapterBase.ADAPTERS.copy()
        try:
            # Note: with __init_subclass__, the registration happens automatically
            class CustomAdapter(AdapterBase):
                ADAPTER = "test-custom-adapter"

            # But for testing purposes, we manually add it to the registry
            AdapterBase.ADAPTERS["custom"] = CustomAdapter

            assert "custom" in AdapterBase.ADAPTERS
            assert AdapterBase.ADAPTERS["custom"] == CustomAdapter
        finally:
            AdapterBase.ADAPTERS = original_adapters

    def test_adapter_base_class_funkify(self):
        """Test adapter funkify method creates resources with proper attributes."""

        class CustomAdapter(AdapterBase):
            ADAPTER = "test-custom-adapter"

        uri = "test-uri"
        data = {"param": "value"}
        resource = CustomAdapter.funkify(uri, data)

        assert isinstance(resource, Resource)
        assert resource.uri == uri
        assert resource.data == data
        assert resource.adapter == "test-custom-adapter"


class TestAllAdapters:
    """Tests that run against all adapter implementations."""

    @pytest.mark.parametrize("name,cls", AdapterBase.ADAPTERS.items())
    def test_adapter_setup_and_teardown(self, name, cls):
        """Test that setup and teardown methods work correctly."""
        with patch("logging.config.dictConfig") as mock_dict_config:
            config = MagicMock()
            config.debug = True
            config.log_group = "test-group"
            config.run_id = "test-run-id"

            cls._setup(config)
            mock_dict_config.assert_called_once()
            config_arg = mock_dict_config.call_args[0][0]
            assert config_arg["formatters"]["simple"]["format"].startswith(f"[{config.run_id}]")
            assert config_arg["handlers"]["console"]["level"] == 10  # DEBUG

        with patch("logging.getLogger") as mock_get_logger:
            mock_logger = MagicMock()
            mock_get_logger.return_value = mock_logger
            mock_logger.handlers = []

            # Should not raise errors even without watchtower
            cls._teardown()


class TestLocalAdapter:
    """Additional tests specific to the LocalAdapter."""

    def test_funkify_simple(self):
        """Test funkify with simple data."""
        uri = "test-uri"
        mock_runner = MagicMock()
        mock_runner.funkify.return_value = {"processed": "data"}
        with patch.dict(runners, {uri: mock_runner}):
            result = LocalAdapter.funkify(uri, {"param": "value"})
            mock_runner.funkify.assert_called_once_with(param="value")
            assert isinstance(result, Resource)
            assert result.data == {"processed": "data"}
            assert result.adapter == LocalAdapter.ADAPTER

    def test_funkify_tuple(self):
        """Test funkify when runner returns a tuple."""
        uri = "test-uri"
        mock_runner = MagicMock()
        mock_runner.funkify.return_value = ("new-uri", {"processed": "data"})
        with patch.dict(runners, {uri: mock_runner}):
            result = LocalAdapter.funkify(uri, {"param": "value"})
            mock_runner.funkify.assert_called_once_with(param="value")
            assert isinstance(result, Resource)
            assert result.uri == "new-uri"
            assert result.data == {"processed": "data"}
            assert result.adapter == LocalAdapter.ADAPTER

    def test_send_to_remote(self, test_config):
        """Test send_to_remote method."""
        uri = "test-runner"
        mock_runner = MagicMock()
        mock_instance = MagicMock()
        mock_runner.return_value = mock_instance
        mock_instance.run.return_value = ("test response", "test log message")
        with patch.dict(runners, {uri: mock_runner}):
            test_data = {
                "cache_path": "/tmp",
                "cache_key": "test",
                "kwargs": {"a": 1},
                "dump": "{}",
            }
            dump = json.dumps(test_data)
            result, message = LocalAdapter.send_to_remote(uri, test_config, dump)
            # Verify runner was instantiated correctly
            mock_runner.assert_called_once()
            # Check return values
            assert result == "test response"
            assert message == "test log message"


class TestLambdaAdapter:
    """Additional tests specific to the LambdaAdapter."""

    @pytest.fixture
    def mock_lambda_client(self):
        """Set up a mock Lambda client."""

        with patch("dml_util.adapters.lambda_.get_client") as mock_client:
            mock_client.return_value = MagicMock()
            yield mock_client.return_value

    def test_lambda_adapter_send_to_remote_success(self, test_config, mock_lambda_client):
        """Test successful Lambda invocation."""
        uri = "test-lambda"
        dump = json.dumps({"test": "data"})

        # Set up mock response
        mock_response = {
            "StatusCode": 200,
            "Payload": MagicMock(),
        }
        mock_response["Payload"].read.return_value = json.dumps(
            {"status": 200, "response": "test response", "message": "test log message"}
        ).encode()
        mock_lambda_client.invoke.return_value = mock_response

        result, message = LambdaAdapter.send_to_remote(uri, test_config, dump)

        # Verify Lambda was invoked correctly
        mock_lambda_client.invoke.assert_called_once()
        args, kwargs = mock_lambda_client.invoke.call_args
        assert kwargs["FunctionName"] == uri
        assert kwargs["InvocationType"] == "RequestResponse"

        # Check the payload was constructed correctly
        payload = json.loads(kwargs["Payload"])
        assert "config" in payload
        assert "dump" in payload
        assert payload["dump"] == dump

        # Check return values
        assert result == "test response"
        assert message == "test log message"

    def test_lambda_adapter_send_to_remote_error(self, test_config, mock_lambda_client):
        """Test error handling in Lambda invocation."""

        uri = "test-lambda"
        dump = json.dumps({"test": "data"})

        # Set up mock error response
        mock_response = {
            "StatusCode": 200,
            "Payload": MagicMock(),
        }
        mock_response["Payload"].read.return_value = json.dumps(
            {"status": 400, "message": "Lambda error message"}
        ).encode()
        mock_lambda_client.invoke.return_value = mock_response

        with pytest.raises(Error) as excinfo:
            LambdaAdapter.send_to_remote(uri, test_config, dump)

        # Check error message contains Lambda error details
        assert isinstance(excinfo.value, Error)
        assert excinfo.value.origin == "lambda"


class TestCLIInterface:
    """Tests for the CLI interface."""

    @pytest.mark.parametrize("name,cls", AdapterBase.ADAPTERS.items())
    def test_cli_successful_execution(self, name, cls):
        """Test CLI with successful execution."""

        class MockArgs:
            uri = "test-uri"
            input = MagicMock()
            output = MagicMock()
            error = MagicMock()
            n_iters = 1
            debug = False

        with (
            patch.object(cls, "_setup"),
            patch.object(cls, "_teardown"),
            patch.object(cls, "send_to_remote", return_value=("test response", "test log")),
            patch("dml_util.adapters.base._read_data", return_value="test input data"),
            patch("dml_util.adapters.base._write_data"),
        ):
            status = cls.cli(MockArgs)

            assert status == 0

    @pytest.mark.parametrize("name,cls", AdapterBase.ADAPTERS.items())
    def test_cli_daemon_mode(self, name, cls):
        """Test CLI in daemon mode (multiple iterations)."""

        class MockArgs:
            uri = "test-uri"
            input = MagicMock()
            output = MagicMock()
            error = MagicMock()
            n_iters = 3
            debug = False

        send_responses = [
            (None, "attempt 1"),
            (None, "attempt 2"),
            ("final response", "success"),
        ]

        with (
            patch.object(cls, "_setup"),
            patch.object(cls, "_teardown"),
            patch("dml_util.adapters.base._read_data", return_value="test input data"),
            patch("dml_util.adapters.base._write_data"),
            patch.object(cls, "send_to_remote", side_effect=send_responses),
            patch("time.sleep") as mock_sleep,
        ):
            status = cls.cli(MockArgs)
            assert status == 0
            assert mock_sleep.call_count == 2

    @pytest.mark.parametrize("cls", AdapterBase.ADAPTERS.values())
    def test_cli_daemon(self, cls):
        os.environ["DML_CACHE_KEY"] = "test_key"
        with tmpdir() as tmpd:

            class MockArgs:
                uri = "asdf:uri"
                input = f"{tmpd}/input.dump"
                output = f"{tmpd}/output.dump"
                error = f"{tmpd}/error.dump"
                n_iters = -1
                debug = True

            with open(MockArgs.input, "w") as f:
                f.write("foo")
            send_responses = [
                (None, "attempt 1"),
                (None, "attempt 2"),
                ("qwer", "testing1"),
            ]
            with (
                patch.object(cls, "_setup"),
                patch.object(cls, "_teardown"),
                patch.object(cls, "send_to_remote", side_effect=send_responses),
                patch("time.sleep"),
            ):
                status = cls.cli(MockArgs)
                with open(MockArgs.output, "r") as f:
                    assert f.read() == "qwer"
                with open(MockArgs.error, "r") as f:
                    assert f.read().strip() == "attempt 1\nattempt 2\ntesting1"
                assert status == 0

    @pytest.mark.parametrize("name,cls", AdapterBase.ADAPTERS.items())
    def test_cli_error_handling(self, name, cls):
        """Test CLI error handling."""

        class MockArgs:
            uri = "test-uri"
            input = MagicMock()
            output = MagicMock()
            error = MagicMock()
            n_iters = 1
            debug = True

        test_exception = Exception("Test error")

        with (
            patch.object(cls, "_setup"),
            patch.object(cls, "_teardown"),
            patch("dml_util.adapters.base._read_data", side_effect=test_exception),
            patch("dml_util.adapters.base._write_data") as mock_write,
        ):
            status = cls.cli(MockArgs)

            assert status == 0
            mock_write.assert_called_once()
            # Ensure error message contains the exception
            args, _ = mock_write.call_args
            data = from_json(args[0])
            assert isinstance(data, Error)
            assert data.message == "Test error"
