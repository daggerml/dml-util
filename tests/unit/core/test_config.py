"""Tests for the core config module."""

import json

from dml_util.core.config import EnvConfig, InputConfig
from tests.conftest import CACHE_KEY, CACHE_PATH, S3_BUCKET, S3_PREFIX


class TestEnvConfig:
    """Tests for EnvConfig class."""

    def test_from_env(self):
        """Test creating EnvConfig from environment variables."""
        config = EnvConfig.from_env(debug=True)

        assert config.s3_bucket == S3_BUCKET
        assert config.s3_prefix == S3_PREFIX
        assert config.debug is True
        assert config.log_group == "dml"  # default value
        assert config.run_id  # should be generated

    def test_dumps(self):
        """Test serializing EnvConfig to JSON."""
        c1 = EnvConfig(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            log_group="test-group",
            run_id="test-run-id",
            debug=True,
        )
        c2 = EnvConfig.loads(c1.dumps())
        assert c2 == c1

    def test_to_envvars(self):
        """Test converting EnvConfig to environment variables."""
        config = EnvConfig(
            s3_bucket=S3_BUCKET,
            s3_prefix=S3_PREFIX,
            log_group="test-group",
            run_id="test-run-id",
            debug=True,
        )

        env_vars = config.to_envvars()
        assert env_vars["DML_S3_BUCKET"] == S3_BUCKET
        assert env_vars["DML_S3_PREFIX"] == S3_PREFIX
        assert env_vars["DML_LOG_GROUP"] == "test-group"
        assert env_vars["DML_RUN_ID"] == "test-run-id"
        assert env_vars["DML_DEBUG"] == "1"

        # Test with debug=False
        config.debug = False
        env_vars = config.to_envvars()
        assert "DML_DEBUG" not in env_vars


class TestInputConfig:
    """Tests for InputConfig class."""

    def test_get_sub(self):
        """Test get_sub method."""
        input_config = InputConfig(
            cache_path=CACHE_PATH,
            cache_key=CACHE_KEY,
            kwargs={
                "sub": {
                    "adapter": "test-adapter",
                    "uri": "test-uri",
                    "data": {"param": "value"},
                }
            },
            dump="test-dump",
        )

        adapter, uri, ks_json = input_config.get_sub()
        assert adapter == "test-adapter"
        assert uri == "test-uri"

        # Check the resulting JSON can be loaded
        ks = json.loads(ks_json)
        assert ks["cache_path"] == CACHE_PATH
        assert ks["cache_key"] == CACHE_KEY
        assert ks["kwargs"] == {"param": "value"}
        assert ks["dump"] == "test-dump"
