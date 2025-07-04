"""AWS utilities."""

import logging

import boto3
from botocore.client import Config

logger = logging.getLogger(__name__)


def get_client(name):
    """Get a boto3 client with standard configuration."""
    logger.info("getting %r client", name)
    config = Config(connect_timeout=5, retries={"max_attempts": 5, "mode": "adaptive"})
    return boto3.client(name, config=config)


def maybe_get_client(name):
    """Get a boto3 client, handling exceptions."""
    try:
        return get_client(name)
    except Exception:
        logger.debug("failed to get client %s", name)
        return None
