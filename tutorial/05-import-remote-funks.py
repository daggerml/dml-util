#!/usr/bin/env python3
__doc__ = """
# Example importing a remote batch function and executing it in parallel

Note that the details of how `batch_fn` is implemented are not important here.
You can code up some magic and then use it in a consistent way across all of
your code. If you ever need to know how it was implemented, you can just check
out the dag that defined it.

Note that in our example, the first run will fail because we're dividing by
zero. This is intentional, and shows how you can catch and persist errors.
"""
from concurrent.futures import ThreadPoolExecutor

from daggerml import Dml, Error

if __name__ == "__main__":
    dml = Dml()
    with dml.new("example-importing-batch", __doc__) as dag:
        dag.fn = dml.load("example-on-batch").batch_fn
        good_results = []
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(dag.fn, *range(5), i): i for i in range(5)}
            for future in futures:
                try:
                    result = future.result()
                    dag[f"result_{futures[future]}"] = result
                    good_results.append(result)
                except Error as e:
                    print(f"Error caught: {e}")
        dag.result = good_results
