import json
import sys
from uuid import uuid4

from daggerml import Dml

if __name__ == "__main__":
    stdin = json.loads(sys.stdin.read())
    with Dml.temporary(cache_path=stdin["cache_path"]) as dml:
        with dml.new("test", "test", stdin["dump"], print) as dag:
            dag.put(len(dag.argv[1:]), name="num_args")
            dag.put(sum(dag.argv[1:].value()), name="n0")
            dag.put(str(uuid4()), name="uuid")
            dag.commit(dag.n0)
