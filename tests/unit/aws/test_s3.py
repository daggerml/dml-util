"""Unit tests for the S3Store module."""

from unittest import skipUnless
from unittest.mock import Mock, patch

import pytest
from botocore.exceptions import ClientError

from dml_util.aws.s3 import S3Store
from dml_util.core.daggerml import Node, Resource, has_daggerml
from tests.util import S3_BUCKET, S3_PREFIX, _root_, ls_r, tmpdir


class TestS3Store:
    """Tests for the S3Store class.

    These tests verify the functionality of the S3Store class
    for interacting with AWS S3 storage.
    """

    def test_s3store_initialization(self):
        """Test S3Store initialization with various parameters.

        This test verifies that S3Store properly initializes with:
        1. Default parameters from environment variables
        2. Explicitly provided bucket and prefix
        """
        with patch("boto3.client"):
            # Test initialization with environment variables
            s3 = S3Store()
            assert s3.bucket == S3_BUCKET  # From environment setup
            assert s3.prefix == f"{S3_PREFIX}/data"  # From environment setup

            # Test initialization with explicit parameters
            custom_bucket = "custom-bucket"
            custom_prefix = "custom-prefix"
            s3 = S3Store(bucket=custom_bucket, prefix=custom_prefix)
            assert s3.bucket == custom_bucket
            assert s3.prefix == custom_prefix

    def test_name2uri(self):
        """Test conversion of name to S3 URI.

        This test verifies that _name2uri correctly:
        1. Converts a simple name to a properly formatted S3 URI
        2. Includes the bucket and prefix in the URI
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            # Test simple name
            uri = s3._name2uri("test.txt")
            assert uri.startswith("s3://test-bucket/test-prefix/")
            assert uri.endswith("/test.txt")

            # Test with nested path
            uri = s3._name2uri("folder/test.txt")
            assert uri.startswith("s3://test-bucket/test-prefix/")
            assert uri.endswith("/folder/test.txt")

    @pytest.mark.parametrize(
        "name_or_uri,expected_bucket,expected_key",
        [
            # Simple name
            ("test.txt", "test-bucket", "test-prefix/test.txt"),
            # Full S3 URI
            ("s3://other-bucket/some/key.txt", "other-bucket", "some/key.txt"),
            # Resource object
            (
                Resource("s3://other-bucket/some/key.txt"),
                "other-bucket",
                "some/key.txt",
            ),
            # Path with directories
            ("dir/subdir/file.txt", "test-bucket", "test-prefix/dir/subdir/file.txt"),
        ],
    )
    def test_parse_uri(self, name_or_uri, expected_bucket, expected_key):
        """Test parsing URIs and names into bucket and key components.

        This test verifies that S3Store correctly handles different URI formats:
        1. Simple filename
        2. Full S3 URI
        3. Resource object
        4. Path with directories
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")
            bucket, key = s3.parse_uri(name_or_uri)
            assert bucket == expected_bucket
            assert key == expected_key

    def test_parse_uri_with_resource(self):
        """Test parsing URIs from Resource objects.

        This test verifies that S3Store correctly handles Resource objects
        when parsing URIs.
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            uri = "s3://other-bucket/some/key.txt"
            obj = Resource(uri)
            bucket, key = s3.parse_uri(obj)

            assert bucket == "other-bucket"
            assert key == "some/key.txt"

    def test_parse_uri_with_node(self):
        """Test parsing URIs from Node objects.

        This test verifies that S3Store correctly handles Node objects
        when parsing URIs.
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            uri = "s3://other-bucket/some/key.txt"
            # Mock Node object and its value() method
            mock_node = Mock(spec=Node)
            mock_node.value.return_value = uri

            bucket, key = s3.parse_uri(mock_node)

            assert bucket == "other-bucket"
            assert key == "some/key.txt"

    def test_uri_parsing(self):
        """Test URI parsing functionality.

        This test verifies that S3Store correctly:
        1. Extracts the key portion from an S3 URI
        2. Validates the URI format and bucket
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            # Test parsing S3 URI components
            uri = "s3://test-bucket/test-prefix/test.txt"

            # Verify we can get the file from an URI without errors
            with patch.object(s3, "get", return_value=b"test content"):
                try:
                    s3.get(uri)
                    # If we get here, no exceptions were raised
                    parsing_succeeded = True
                except Exception:
                    parsing_succeeded = False

                assert parsing_succeeded, "URI parsing failed"

    def test_cd(self):
        """Test changing the current directory in S3Store.

        This test verifies that S3Store can change its current directory
        and correctly updates the prefix for subsequent operations.
        """
        s3 = S3Store(bucket="test-bucket", prefix="test-prefix/foo")
        assert s3.cd("bar").prefix == "test-prefix/foo/bar"
        assert s3.prefix == "test-prefix/foo"
        assert s3.cd("..").prefix == "test-prefix"

    @pytest.mark.parametrize(
        "object_exists,error_response",
        [
            (True, None),  # Object exists, no error
            (False, {"Error": {"Code": "404"}}),  # Object does not exist, 404 error
        ],
    )
    def test_exists(self, object_exists, error_response):
        """Test checking if an object exists in S3.

        This test verifies that the exists method correctly:
        1. Returns True when the object exists
        2. Returns False when the object doesn't exist (404 error)
        3. Raises an exception for other errors
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            if object_exists:
                # Set up mock to indicate object exists
                s3.client.head_object.return_value = {"ContentLength": 100}
                assert s3.exists("test-file.txt") is True
            else:
                # Set up mock to raise ClientError with 404
                err_response = error_response
                mock_exception = ClientError(err_response, "head_object")
                s3.client.head_object.side_effect = mock_exception

                if err_response.get("Error", {}).get("Code") == "404":
                    # Should return False for 404
                    assert s3.exists("test-file.txt") is False
                else:
                    # Should re-raise other errors
                    with pytest.raises(ClientError):
                        s3.exists("test-file.txt")

    def test_exists_other_error(self):
        """Test exists method behavior with non-404 errors.

        This test verifies that the exists method raises an exception
        when encountering errors other than 404 Not Found.
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            # Set up mock to raise ClientError with non-404 error
            err_response = {"Error": {"Code": "403", "Message": "Access Denied"}}
            mock_exception = ClientError(err_response, "head_object")
            s3.client.head_object.side_effect = mock_exception

            # Should raise the exception for non-404 errors
            with pytest.raises(ClientError):
                s3.exists("test-file.txt")

    @pytest.mark.parametrize(
        "name_or_uri,expected_bucket,expected_key",
        [
            # Simple filename
            ("test.txt", "test-bucket", "test-prefix/test.txt"),
            # Full S3 URI
            ("s3://other-bucket/some/key.txt", "other-bucket", "some/key.txt"),
        ],
    )
    def test_get(self, name_or_uri, expected_bucket, expected_key):
        """Test getting an object from S3.

        This test verifies that the get method correctly:
        1. Parses the provided name or URI to get bucket and key
        2. Calls get_object with the correct parameters
        3. Returns the contents of the object
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            # Mock the get_object response
            mock_body = Mock()
            mock_body.read.return_value = b"test file content"
            s3.client.get_object.return_value = {"Body": mock_body}

            # Call get and verify the result
            result = s3.get(name_or_uri)

            # Verify the correct parameters were passed to get_object
            s3.client.get_object.assert_called_once_with(Bucket=expected_bucket, Key=expected_key)

            # Verify the content was read from the response body
            assert result == b"test file content"
            mock_body.read.assert_called_once()

    def test_get_with_error(self):
        """Test get method behavior with errors.

        This test verifies that the get method properly propagates errors
        from the underlying S3 client.
        """
        with patch("boto3.client"):
            s3 = S3Store(bucket="test-bucket", prefix="test-prefix")

            # Set up mock to raise ClientError
            err_response = {
                "Error": {
                    "Code": "NoSuchKey",
                    "Message": "The specified key does not exist.",
                }
            }
            mock_exception = ClientError(err_response, "get_object")
            s3.client.get_object.side_effect = mock_exception

            # Should raise the exception
            with pytest.raises(ClientError) as exc_info:
                s3.get("test-file.txt")

            # Verify it's the same exception
            assert exc_info.value == mock_exception


class TestWMoto:
    @pytest.mark.usefixtures("s3_bucket")
    def test_js(self):
        s3 = S3Store(bucket=S3_BUCKET, prefix=S3_PREFIX)
        js = {"asdf": "wef", "as": [32, True]}
        resp = s3.put_js(js)
        if not isinstance(resp, str):
            resp = resp.uri  # Resource = str if no dml
        js2 = s3.get_js(resp)
        assert js == js2

    @pytest.mark.usefixtures("s3_bucket")
    def test_ls(self):
        s3 = S3Store(bucket=S3_BUCKET, prefix=S3_PREFIX)
        assert s3.ls(recursive=True) == []
        keys = ["a", "b/c", "b/d", "b/d/e", "f"]
        for key in keys:
            s3.put(b"a", name=key)
        ls = s3.ls(recursive=False, lazy=True)
        assert not isinstance(ls, list)
        assert list(ls) == [s3._name2uri(x) for x in keys if "/" not in x]
        ls = s3.ls(recursive=True)
        assert ls == [s3._name2uri(x) for x in keys]
        assert s3.ls(f"s3://{s3.bucket}/{s3.prefix}/b", recursive=True) == s3.ls("b", recursive=True)
        assert s3.ls("b/", recursive=True) == s3.ls("b", recursive=True)
        assert s3.ls("b", recursive=True) == [s3._name2uri(x) for x in ["b/c", "b/d", "b/d/e"]]
        [s3.rm(k) for k in keys]
        assert s3.ls(recursive=True) == []

    @pytest.mark.usefixtures("s3_bucket")
    def test_ls_recursive_eager(self):
        """Test listing objects recursively with eager loading."""
        s3 = S3Store(bucket=S3_BUCKET, prefix=S3_PREFIX)
        assert s3.ls(recursive=True) == []

        # Create test files
        keys = ["a", "b/c", "b/d", "b/d/e", "f"]
        for key in keys:
            s3.put(b"a", name=key)

        # Test ls with recursive=True, lazy=False
        result = s3.ls(recursive=True, lazy=False)

        # Verify result type is list
        assert isinstance(result, list)

        # Verify all files are returned
        expected_keys = [s3._name2uri(k) for k in keys]
        assert sorted(result) == sorted(expected_keys)

        # Clean up test files
        [s3.rm(k) for k in keys]
        assert s3.ls(recursive=True) == []

    @pytest.mark.usefixtures("s3_bucket")
    def test_ls_recursive_lazy(self):
        """Test listing objects recursively with lazy loading."""
        s3 = S3Store(bucket=S3_BUCKET, prefix=S3_PREFIX)
        assert s3.ls(recursive=True) == []

        # Create test files
        keys = ["a", "b/c", "b/d", "b/d/e", "f"]
        for key in keys:
            s3.put(b"a", name=key)

        # Test ls with recursive=True, lazy=True
        result = s3.ls(recursive=True, lazy=True)

        # Verify result type is generator
        assert not isinstance(result, list)

        # Convert generator to list
        result_list = list(result)

        # Verify all files are returned
        expected_keys = [s3._name2uri(k) for k in keys]
        assert sorted(result_list) == sorted(expected_keys)

        # Clean up test files
        [s3.rm(k) for k in keys]
        assert s3.ls(recursive=True) == []

    @pytest.mark.usefixtures("s3_bucket")
    def test_ls_non_recursive_eager(self):
        """Test listing objects non-recursively with eager loading."""
        s3 = S3Store(bucket=S3_BUCKET, prefix=S3_PREFIX)
        assert s3.ls(recursive=True) == []

        # Create test files
        keys = ["a", "b/c", "b/d", "b/d/e", "f"]
        for key in keys:
            s3.put(b"a", name=key)

        # Test ls with recursive=False, lazy=False
        result = s3.ls(recursive=False, lazy=False)

        # Verify result type is list
        assert isinstance(result, list)

        # Verify only top-level files are returned
        expected_keys = [s3._name2uri(k) for k in keys if "/" not in k]
        assert sorted(result) == sorted(expected_keys)

        # Clean up test files
        [s3.rm(k) for k in keys]
        assert s3.ls(recursive=True) == []

    @pytest.mark.usefixtures("s3_bucket")
    def test_ls_non_recursive_lazy(self):
        """Test listing objects non-recursively with lazy loading."""
        s3 = S3Store(bucket=S3_BUCKET, prefix=S3_PREFIX)
        assert s3.ls(recursive=True) == []

        # Create test files
        keys = ["a", "b/c", "b/d", "b/d/e", "f"]
        for key in keys:
            s3.put(b"a", name=key)

        # Test ls with recursive=False, lazy=True
        result = s3.ls(recursive=False, lazy=True)

        # Verify result type is generator
        assert not isinstance(result, list)

        # Convert generator to list
        result_list = list(result)

        # Verify only top-level files are returned
        expected_keys = [s3._name2uri(k) for k in keys if "/" not in k]
        assert sorted(result_list) == sorted(expected_keys)

        # Clean up test files
        [s3.rm(k) for k in keys]
        assert s3.ls(recursive=True) == []

    @skipUnless(has_daggerml, "Dml not available")
    @pytest.mark.usefixtures("s3_bucket")
    def test_tar(self):
        from daggerml import Dml

        context = _root_ / "tests/assets/dkr-context"
        s3 = S3Store(bucket=S3_BUCKET, prefix=S3_PREFIX)
        assert s3.bucket == S3_BUCKET
        assert s3.prefix.startswith(S3_PREFIX)
        with Dml.temporary() as dml:
            s3_tar = s3.tar(dml, context)
            with tmpdir() as tmpd:
                s3.untar(s3_tar, tmpd)
                assert ls_r(tmpd) == ls_r(context)
            # consistent hash
            s3_tar2 = s3.tar(dml, context)
            assert s3_tar.uri == s3_tar2.uri
