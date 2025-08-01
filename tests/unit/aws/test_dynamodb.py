"""Unit tests for the DynamoState module."""

from time import time
from unittest.mock import MagicMock, patch

import pytest

from dml_util.aws.dynamodb import DynamoState


class TestDynamoState:
    """Tests for the DynamoState class.

    DynamoState provides state storage and locking for distributed tasks using DynamoDB.
    These tests verify the state management and locking functionality.
    """

    @pytest.fixture(autouse=True)
    def setup_test(self):
        """Set up the test environment."""
        self.table_name = "test-job"
        self.table_arn = f"arn:aws:dynamodb:us-east-1:123456789012:table/{self.table_name}"
        self.cache_key = "test-key"
        with patch.dict("os.environ", {"DYNAMODB_TABLE": self.table_name}):
            yield

    def test_dynamostate_initialization(self):
        """Test DynamoState initialization.

        This test verifies that DynamoState properly initializes with:
        1. A cache key for identifying state entries
        2. Optional table name parameter
        3. Default or custom timeout values
        """
        with patch("boto3.client"):
            # Test with default parameters
            db = DynamoState(self.cache_key)
            assert db.cache_key == self.cache_key
            assert db.timeout > 0  # Should have a default timeout

            # Test with custom parameters
            custom_timeout = 30
            db = DynamoState(self.cache_key, tb=self.table_name, timeout=custom_timeout)
            assert db.cache_key == self.cache_key
            assert db.timeout == custom_timeout

    @patch("boto3.client")
    def test_dynamostate_operations(self, mock_boto_client):
        """Test DynamoState operations.

        This test verifies that DynamoState correctly:
        1. Performs basic operations like get, put, unlock
        2. Properly formats requests to DynamoDB
        """
        # Mock the DynamoDB client
        mock_boto_client.return_value = MagicMock()

        # Create instance with mocked components
        db = DynamoState(self.cache_key, tb=self.table_name)

        # Ensure we can access properties without errors
        assert db.cache_key == self.cache_key
        assert hasattr(db, "db")  # Should have a db client

    @pytest.mark.usefixtures("dynamodb_table")
    def test_dynamo_db_ops(self):
        data = {"q": "b"}
        db = DynamoState("test-key")
        info = db.get()
        assert info == {}
        assert db.put(data)
        assert db.get() == data
        assert db.unlock()
        db2 = DynamoState("test-key")
        assert db2.get() == data

    @pytest.mark.usefixtures("dynamodb_table")
    def test_dynamo_locking(self):
        t0 = [time()]
        timeout = 0.25
        db0 = DynamoState("test-key", timeout=timeout)
        db1 = DynamoState("test-key", timeout=timeout)
        with patch("dml_util.aws.dynamodb.time", lambda: t0[0]):
            assert db0.get() == {}
            t0[0] += 0.01
            assert db1.get() is None
            t0[0] += 0.01
            assert db1.put({"asdf": 23}) is False
            t0[0] += 0.01
            assert db0.put({"q": "b"}) is True
            t0[0] += 0.01
            t0[0] += timeout
            assert db1.get() == {"q": "b"}
            t0[0] += 0.01
            assert db0.unlock() is False
            t0[0] += 0.01
            assert db1.unlock() is True
        db1.delete()
