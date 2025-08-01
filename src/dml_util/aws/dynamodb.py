"""DynamoDB state management.

This module provides state management functionality using AWS DynamoDB.
It includes classes for storing and retrieving state with locking mechanisms
to ensure data consistency in distributed environments.
"""

import json
import logging
import os
from dataclasses import dataclass, field
from typing import Union
from uuid import uuid4

import boto3

from dml_util.aws import get_client
from dml_util.core.state import TIMEOUT, State
from dml_util.core.utils import js_dump, now

logger = logging.getLogger(__name__)


@dataclass
class DynamoState(State):
    """DynamoDB-based state management.

    This class implements state management using AWS DynamoDB. It provides
    methods for storing and retrieving state with locking capabilities to ensure
    data consistency in distributed environments.

    Parameters
    ----------
    cache_key : str
        Unique identifier for the state record in DynamoDB.
    run_id : str, optional
        Unique identifier for the current run, used for locking.
        Defaults to a random UUID.
    timeout : int, float, optional
        Lock timeout in seconds. Defaults to TIMEOUT (5 seconds).
    db : boto3.client, optional
        DynamoDB client. Defaults to a new client created using get_client.
    tb : str, optional
        DynamoDB table name. Defaults to the value of the environment
        variable DYNAMODB_TABLE.
    """

    cache_key: str
    run_id: str = field(default_factory=lambda: uuid4().hex)
    timeout: Union[int, float] = field(default=TIMEOUT)
    db: "boto3.client" = field(default_factory=lambda: get_client("dynamodb"))
    tb: str = field(default_factory=lambda: os.environ["DYNAMODB_TABLE"])

    def _update(self, key=None, **kw):
        try:
            return self.db.update_item(
                TableName=self.tb,
                Key={"cache_key": {"S": key or self.cache_key}},
                **kw,
            )
        except Exception as e:
            if getattr(e, "response", {}).get("Error", {}).get("Code") == "ConditionalCheckFailedException":
                logger.info("could not update %r (invalid lock)", self.cache_key)
                return
            raise

    def get(self, key=None):
        """
        returns:
            None if could not acquire lock
            {} if there's no data
            data otherwise
        """
        logger.info("acquiring lock for %r", self.cache_key)
        ut = now()
        resp = self._update(
            key,
            UpdateExpression="SET #lk = :lk, #ut = :ut",
            ConditionExpression="attribute_not_exists(#lk) OR #lk = :lk OR #ut < :to",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#ut": "update_time",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":ut": {"N": str(ut)},
                ":to": {"N": str(ut - self.timeout)},
            },
            ReturnValues="ALL_NEW",
        )
        if resp is None:
            return
        obj = resp["Attributes"].get("obj", {})
        return obj and json.loads(obj["S"])

    def put(self, obj):
        logger.info("putting data for %r", self.cache_key)
        resp = self._update(
            UpdateExpression="SET #obj = :obj, #ut = :ut",
            ConditionExpression="#lk = :lk",
            ExpressionAttributeNames={
                "#lk": "lock_key",
                "#obj": "obj",
                "#ut": "update_time",
            },
            ExpressionAttributeValues={
                ":lk": {"S": self.run_id},
                ":obj": {"S": js_dump(obj)},
                ":ut": {"N": str(round(now(), 2))},
            },
        )
        return resp is not None

    def unlock(self, key=None):
        logger.info("releasing lock for %r", self.cache_key)
        try:
            resp = self._update(
                key,
                UpdateExpression="REMOVE #lk",
                ConditionExpression="#lk = :lk",
                ExpressionAttributeNames={"#lk": "lock_key"},
                ExpressionAttributeValues={":lk": {"S": self.run_id}},
            )
            return resp is not None
        except Exception:
            pass

    def delete(self):
        try:
            return self.db.delete_item(
                TableName=self.tb,
                Key={"cache_key": {"S": self.cache_key}},
                ConditionExpression="#lk = :lk",
                ExpressionAttributeNames={"#lk": "lock_key"},
                ExpressionAttributeValues={":lk": {"S": self.run_id}},
            )
        except Exception as e:
            if getattr(e, "response", {}).get("Error", {}).get("Code") != "ConditionalCheckFailedException":
                raise
