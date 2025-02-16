# What is this?

We're going to make a batch cluster that is set up to be used in daggerml.
The easiest way to do this is to write a lambda that can handle all the
daggerml calls.

To get started, create and activate a virtual env and run
`python -m pip install -r requirements.txt`.

To run the setup, run `python ./batch_dag.py`. Internally, this will read
`entrypoint.py` and insert it into the appropriate spot in `cf.json`, and then
deploy that stack to cloudformation using boto3.

Next to use the cluster (in the simplest way), we'll use a standard python3.12
docker image and compute the sum in batch.

To do that, we'll run `python ./example.py`. Internally, that will read
`example_script.py` and submit that script to our aws batch cluster.
