import json
import os
import subprocess

import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError
from daggerml import Dml, Resource
from daggerml.core import Dag, Node, Ref
from flask import Flask, abort, render_template, request, send_from_directory, url_for

from dml_util.baseutil import S3_BUCKET, S3_PREFIX, S3Store

app = Flask(__name__)
ROOT_DIR = os.path.join(os.getcwd(), "root")


@app.route("/")
def index():
    dml = Dml()
    repos = [x["name"] for x in dml("repo", "list")]
    dropdowns = {"Repos": {x: url_for("repo_page", repo=x) for x in repos}}
    return render_template("index.html", dropdowns=dropdowns)


@app.route("/repo:<repo>")
def repo_page(repo):
    dml = Dml(repo=repo)
    repos = [x["name"] for x in dml("repo", "list")]
    branches = [x for x in dml("branch", "list")]
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        "Branches": {x: url_for("branch_page", repo=repo, branch=x) for x in branches},
    }
    return render_template("index.html", dropdowns=dropdowns)


@app.route("/repo:<repo>/branch:<branch>")
def branch_page(repo, branch):
    dml = Dml(repo=repo, branch=branch)
    repos = [x["name"] for x in dml("repo", "list")]
    branches = [x for x in dml("branch", "list")]
    dags = [x["name"] for x in dml("dag", "list")]
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        f"Branches({branch})": {x: url_for("branch_page", repo=repo, branch=x) for x in branches},
        "Dag": {x: url_for("dag_page", repo=repo, branch=branch, dag_id=x) for x in dags},
    }
    return render_template("index.html", dropdowns=dropdowns)


@app.route("/repo:<repo>/branch:<branch>/dag:<dag_id>")
def dag_page(repo, branch, dag_id):
    dml = Dml(repo=repo, branch=branch)
    dml = Dml(repo=repo, branch=branch)
    repos = [x["name"] for x in dml("repo", "list")]
    branches = [x for x in dml("branch", "list")]
    dags = [x["name"] for x in dml("dag", "list")]
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        f"Branches({branch})": {x: url_for("branch_page", repo=repo, branch=x) for x in branches},
        f"Dag({dag_id})": {x: url_for("dag_page", repo=repo, branch=branch, dag_id=x) for x in dags},
    }
    dag_data = dml("dag", "graph", "--output", "json", dag_id)
    from pprint import pprint

    pprint(dag_data)
    for node in dag_data["nodes"]:
        node["link"] = url_for(
            "node_page",
            repo=repo,
            branch=branch,
            dag_id=dag_id,
            node_id=node["name"] or "",
        )
    return render_template(
        "dag.html",
        dropdowns=dropdowns,
        data=json.dumps(dag_data),
    )


@app.route("/repo:<repo>/branch:<branch>/dag:<dag_id>/node:<node_id>")
def node_page(repo, branch, dag_id, node_id):
    dml = Dml(repo=repo, branch=branch)
    dml = Dml(repo=repo, branch=branch)
    repos = [x["name"] for x in dml("repo", "list")]
    branches = [x for x in dml("branch", "list")]
    dags = [x["name"] for x in dml("dag", "list")]
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        f"Branches({branch})": {x: url_for("branch_page", repo=repo, branch=x) for x in branches},
        f"Dag({dag_id})": {x: url_for("dag_page", repo=repo, branch=branch, dag_id=x) for x in dags},
    }
    val = dml.load(dag_id)[node_id].value()
    if not isinstance(val, Resource):
        abort(501, description="Dashboards only implemented for resource nodes.")
    s3 = S3Store()
    bucket, key = s3.parse_uri(val.uri)
    if bucket is None:
        abort(501, description="URI is not on s3.")
    if not key.endswith(".html"):
        abort(501, description=f"{val.uri!r} is not an html file")
    notebook_url = s3.client.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=3600,
    )
    return render_template(
        "node.html",
        dropdowns=dropdowns,
        notebook_url=notebook_url,
    )


@app.route("/view_notebook/<notebook_key>")
def view_notebook(notebook_key):
    s3_client = boto3.client("s3")
    bucket_name = "your-private-bucket-name"

    try:
        presigned_url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket_name, "Key": notebook_key},
            ExpiresIn=3600,  # URL expires in 1 hour
        )
    except (NoCredentialsError, PartialCredentialsError):
        abort(403, description="Access to S3 bucket is forbidden.")
    except s3_client.exceptions.NoSuchKey:
        abort(404, description="Notebook not found.")
    except Exception as e:
        abort(500, description=str(e))

    return render_template("notebook_viewer.html", notebook_url=presigned_url)


if __name__ == "__main__":
    app.run(debug=True)
