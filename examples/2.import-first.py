#!/usr/bin/env python3
"""# My second cool example

## Overview
We're just going to load the results from `first_dag`, and return the leaves + 1

## And we can use markdown!

```python
def inc(x):
    return x + 1
```

$$
y = mx + b
$$

And the UI will display this accordingly.
"""
import argparse
import getpass
import logging

from daggerml import Dml

from dml_util import funkify

logger = logging.getLogger(__name__)


@funkify
def add_numbers(dag):
    """Simple function that adds numbers together"""
    def leaf_map(x, c):
        if isinstance(x, list):
            return [leaf_map(i, c) for i in x]
        elif isinstance(x, dict):
            return {k: leaf_map(v, c) for k, v in x.items()}
        return x + c if x is not None else None
    return leaf_map(dag.argv[1].value(), dag.argv[2].value())


def create_sample_dags(dml):
    """Create sample DAGs with different functions and data flows"""
    logger.info("Creating sample DAGs...")
    with dml.new("second_dag", __doc__) as dag:
        logger.info("Instantiating datasets")
        first_result = dag._put(dml.load("first_dag").result, name="first_result")
        add_fn = dag._put(add_numbers, name="add_numbers")
        dag.result = [add_fn(first_result, x, name=f"add_{x}") for x in range(1, 6)]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create sample DAGs for DaggerML")
    parser.add_argument("--config_dir", type=str, default=None, help="Directory for DaggerML configuration")
    parser.add_argument("--repo", type=str, default=None, help="DaggerML repository path")
    parser.add_argument("--user", type=str, default=getpass.getuser(), help="DaggerML user name (optional)")
    args = parser.parse_args()

    dml = Dml(config_dir=args.config_dir, repo=args.repo, user=args.user)
    create_sample_dags(dml)

