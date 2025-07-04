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

    @pytest.mark.parametrize(
        "prefix,name,expected_uri",
        [
            ("test-prefix", "test.txt", "s3://test-bucket/test-prefix/test.txt"),
            ("test-prefix", "/test.txt", "s3://test-bucket/test-prefix//test.txt"),  # you can have double slashes
            ("test-prefix", "folder/test.txt", "s3://test-bucket/test-prefix/folder/test.txt"),
            ("", "test.txt", "s3://test-bucket/test.txt"),
            ("test-prefix/", "test.txt", "s3://test-bucket/test-prefix/test.txt"),
            ("test-prefix/subdir", "test.txt", "s3://test-bucket/test-prefix/subdir/test.txt"),
            ("", "s3://other-bucket/some/key.txt", "s3://other-bucket/some/key.txt"),  # full S3 URI
            ("test-prefix", Resource("s3://other-bucket/some/key.txt"), "s3://other-bucket/some/key.txt"),  # Resource object
        ],
    )
    def test_name2uri(self, prefix, name, expected_uri):
        s3 = S3Store(bucket="test-bucket", prefix=prefix)
        uri = s3._name2uri(name)
        assert uri == expected_uri

    @pytest.mark.parametrize(
        "name_or_uri,expected_bucket,expected_key",
        [
            ("test.txt", "test-bucket", "test-prefix/test.txt"),
            ("s3://other-bucket/some/key.txt", "other-bucket", "some/key.txt"),
            (Resource("s3://other-bucket/some/key.txt"), "other-bucket", "some/key.txt"),
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
        s3 = S3Store(bucket="test-bucket", prefix="test-prefix")
        bucket, key = s3.parse_uri(name_or_uri)
        assert bucket == expected_bucket
        assert key == expected_key

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

    @pytest.mark.parametrize(
        "root,change,expected",
        [
            ("test-prefix", "foo", "test-prefix/foo"),
            ("test-prefix/foo", "bar", "test-prefix/foo/bar"),
            ("test-prefix", "foo/bar", "test-prefix/foo/bar"),
            ("test-prefix/foo/bar", "..", "test-prefix/foo"),
            ("test-prefix/foo/bar", "../..", "test-prefix"),
            ("test-prefix/foo/bar", "../../..", ""),
            ("", "foo/bar", "foo/bar"),
        ],
    )
    def test_cd(self, root, change, expected):
        s3 = S3Store(bucket="test-bucket", prefix=root)
        assert s3.cd(change).prefix == expected

    @pytest.mark.usefixtures("s3_bucket")
    @pytest.mark.parametrize(
        "prefix,name,uri,expected_key",
        [
            ("test-prefix", "test.txt", None, "test-prefix/test.txt"),
            ("test-prefix/", "test.txt", None, "test-prefix/test.txt"),
            ("test-prefix/subdir", "test.txt", None, "test-prefix/subdir/test.txt"),
            ("", "test.txt", None, "test.txt"),
            ("asdf", None, f"s3://{S3_BUCKET}/test-prefix/foo.txt", "test-prefix/foo.txt"),
        ],
    )
    def test_put(self, prefix, name, uri, expected_key):
        s3 = S3Store(prefix=prefix)
        assert s3.prefix == prefix.rstrip("/")
        new_uri = s3.put(b"test content", name=name, uri=uri).uri
        assert new_uri == f"s3://{s3.bucket}/{expected_key}"
        assert s3.get(name or uri) == s3.get(new_uri) == b"test content"

    @pytest.mark.parametrize(
        "name_or_uri,expected_bucket,expected_key",
        [
            ("test.txt", "test-bucket", "test-prefix/test.txt"),
            ("s3://other-bucket/some/key.txt", "other-bucket", "some/key.txt"),
        ],
    )
    def test_get(self, name_or_uri, expected_bucket, expected_key):
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
        s3.rm(keys)
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
