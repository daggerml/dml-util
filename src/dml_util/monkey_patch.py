from uuid import uuid4

from daggerml.core import Dag


def call(self, val, name=None):
    name = name or uuid4().hex
    self[name] = val
    return self[name]


Dag.__call__ = call
