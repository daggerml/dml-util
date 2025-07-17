"""AWS utilities."""

import json
import logging
import os
import urllib.request

import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, NoRegionError

logger = logging.getLogger(__name__)


def _get_region_from_metadata():
    """
    Attempts to retrieve the AWS region from ECS or EC2 metadata.

    Returns
    -------
    Optional[str]
        The AWS region string if found, otherwise None.
    """
    # ECS (used in AWS Batch)
    metadata_uri = os.environ.get("ECS_CONTAINER_METADATA_URI_V4") or os.environ.get("ECS_CONTAINER_METADATA_URI")
    if metadata_uri:
        try:
            with urllib.request.urlopen(metadata_uri, timeout=2) as response:
                metadata = json.load(response)
            cluster_label = metadata.get("Labels", {}).get("com.amazonaws.ecs.cluster", "")
            region = cluster_label.split(":")[0] if ":" in cluster_label else None
            if region:
                return region
        except Exception as e:
            logger.warning("Failed to get region from ECS metadata: %s", e)
    # EC2 fallback: use IMDSv2
    try:
        token_req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"}
        )
        with urllib.request.urlopen(token_req, timeout=2) as token_response:
            token = token_response.read().decode()
        region_req = urllib.request.Request(
            "http://169.254.169.254/latest/dynamic/instance-identity/document",
            headers={"X-aws-ec2-metadata-token": token}
        )
        with urllib.request.urlopen(region_req, timeout=2) as region_response:
            identity_doc = json.load(region_response)
        return identity_doc.get("region")
    except Exception as e:
        logger.warning("Failed to get region from EC2 metadata: %s", e)
    return

def get_client(name, region=None, default_region="us-east-1"):
    """
    Creates a robust boto3 client, determining the AWS region in the following order:
        1. Explicit argument
        2. AWS_REGION / AWS_DEFAULT_REGION environment variables
        3. boto3/botocore session
        4. ECS/EC2 metadata
        5. Fallback default region (us-east-1)

    Parameters
    ----------
    name : str
        The name of the AWS service client.
    region : Optional[str], default=None
        The AWS region to use.
    default_region : str, default="us-east-1"
        The fallback AWS region.

    Returns
    -------
    boto3.client
        A boto3 client for the specified service.
    """
    # Step 1–3: Try common boto3 config methods
    region = (
        region or
        os.environ.get("AWS_REGION") or
        os.environ.get("AWS_DEFAULT_REGION")
    )
    if not region:
        try:
            region = boto3.Session().region_name
        except (BotoCoreError, NoRegionError):
            logger.debug("could not instantiate boto client...")
            pass
    # Step 4: Metadata if still no region
    if not region:
        logger.debug("inferring aws region from metadata")
        region = _get_region_from_metadata()
    # Step 5: Fallback default
    if not region:
        logger.warning(f"falling back to default region '{default_region}'")
        region = default_region
    config = Config(
        region_name=region,
        connect_timeout=5,
        retries={"max_attempts": 5, "mode": "adaptive"}
    )
    return boto3.client(name, config=config)

