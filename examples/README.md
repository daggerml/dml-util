# Examples

This repository contains examples of how to use the `dml-util` library to run
code in a distributed and reproducible manner. The examples are very simple and
generally described in the file's docstring. That docstring is also the dag
"message", so you can see them later in the UI (sold separately).

# Running the examples
To run the examples, you should first have dml-util (and daggerml, and
daggerml-cli) installed. To install all of them:

```bash
pip install dml-util[dml]
```

Then set the following environment variables:

```bash
export DML_S3_BUCKET=<your-bucket>
export DML_S3_PREFIX=<your-prefix>
export DML_REPO=test
```

Then set up dml.

```bash
dml repo create test
dml cache create
dml config user $USER
```

Then you should be able to run the examples as python scripts. For example:

```bash
python examples/1.first.py
```
