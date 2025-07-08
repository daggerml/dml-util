#!/usr/bin/env python3
import argparse
import getpass
import logging

from daggerml import Dml

from dml_util import funkify

logger = logging.getLogger(__name__)


@funkify
def add_numbers(dag):
    """Simple function that adds numbers together"""
    import sys

    print("Adding numbers:", dag.argv[1:].value())
    print("Debug information to stderr", file=sys.stderr)
    dag.result = sum(dag.argv[1:].value())
    return dag.result


@funkify
def calculate_statistics(dag):
    """Calculate basic statistics on a list of numbers"""
    import sys
    from statistics import mean, median, stdev

    numbers = dag.argv[1].value()
    print(f"Calculating statistics for {len(numbers)} values", file=sys.stderr)
    if not numbers:
        print("ERROR: Empty input list", file=sys.stderr)
        raise RuntimeError("Cannot calculate statistics on an empty list")
    dag.count = len(numbers)
    dag.sum = sum(numbers)
    dag.mean = mean(numbers)
    dag.median = median(numbers)
    try:
        dag.stdev = stdev(numbers)
    except ValueError:
        print("WARNING: Could not calculate standard deviation", file=sys.stderr)
        dag.stdev = None
    dag.results = {
        "count": dag.count,
        "sum": dag.sum,
        "mean": dag.mean,
        "median": dag.median,
        "stdev": dag.stdev,
    }

    print(f"Results computed: {dag.results}")
    dag.result = dag.results
    return dag.result


@funkify
def process_data(dag):
    """Process data through multiple steps"""
    import sys

    stats_function = dag.argv[1]
    data = dag.argv[2]
    print(f"Processing {len(data)} data points", file=sys.stderr)
    # First transform the data
    dag.transformed = [x * 2 for x in data.value()]
    print(f"Transformed data: {dag.transformed}")
    # Then run statistics on both original and transformed
    dag.original_stats = stats_function(data)
    dag.transformed_stats = stats_function(dag.transformed)
    # Final output is a comparison
    dag.comparison = {
        "original": dag.original_stats,
        "transformed": dag.transformed_stats,
        "scale_factor": 2,
    }
    dag.result = dag.comparison
    return dag.result


def create_sample_dags(dml):
    """Create sample DAGs with different functions and data flows"""
    logger.info("Creating sample DAGs...")
    with dml.new("simple_addition", "Basic fnapp examples") as dag:
        logger.info("Instantiating datasets")
        dag.data = {
            "small": [1, 2, 3, 4, 5],
            "large": list(range(100)),
            "edge_case": [42],  # Single value
        }

        print("Sample data created:", dag.data["small"].value())
        logger.info("Creating simple addition DAG")
        dag.add_fn = add_numbers
        dag.sums = {k: dag.add_fn(*v, name=k) for k, v in dag.data.items()}

        logger.info("Calculating stats")
        dag.stats_fn = calculate_statistics
        dag.stats = {
            "sums": dag.stats_fn([x[1] for x in dag.sums.items()]),
            **{k: dag.stats_fn(v) for k, v in dag.data.items()},
        }

        logger.info("Creating data processing pipeline DAG")
        dag.process_fn = process_data
        procd = {k: dag.process_fn(dag.stats_fn, v) for k, v in dag.data.items()}
        procd["sums"] = dag.process_fn(dag.stats_fn, dag.sums.values())
        dag.processed = procd

        # commit the DAG
        dag.result = {"sums": dag.sums, "stats": dag.stats, "processed": dag.processed}

    created_dags = [x["name"] for x in dml("dag", "list")]
    logger.info("Sample DAGs created successfully: %s", created_dags)
    logger.info("To view the DAGs, run: python dev/run_ui.py")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create sample DAGs for DaggerML")
    parser.add_argument("--config_dir", type=str, default=None, help="Directory for DaggerML configuration")
    parser.add_argument("--repo", type=str, default=".", help="DaggerML repository path")
    parser.add_argument("--user", type=str, default=getpass.getuser(), help="DaggerML user name (optional)")
    args = parser.parse_args()

    dml = Dml(config_dir=args.config_dir, repo=args.repo, user=args.user)
    create_sample_dags(dml)
