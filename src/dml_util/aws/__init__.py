"""AWS utilities."""

import logging
import os

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)


def get_client(name, region=None) -> "boto3.client":
    """Get a boto3 client with standard configuration."""
    # FIXME: This function should figure out the region from the environment
    logger.info("getting %r client", name)
    region = region or os.getenv("AWS_REGION", boto3.Session().region_name or "us-east-1")
    config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "adaptive"}, region_name=region)
    return boto3.client(name, config=config)


def maybe_get_client(name):
    """Get a boto3 client, handling exceptions."""
    try:
        return get_client(name)
    except Exception:
        logger.debug("failed to get client %s", name)
        return None
