import logging
import os
import re
from argparse import ArgumentParser
from pprint import pformat

from daggerml import Dml, Resource
from daggerml.core import Ref
from flask import Flask, abort, render_template, url_for

from dml_util.baseutil import S3Store, tree_map

app = Flask(__name__)
ROOT_DIR = os.path.join(os.getcwd(), "root")
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        "Branches": {x: url_for("branch_page", repo=repo, branch=x) for x in dml("branch", "list")},
    }
    return render_template("index.html", dropdowns=dropdowns)


@app.route("/repo:<repo>/branch:<branch>")
def branch_page(repo, branch):
    dml = Dml(repo=repo, branch=branch)
    repos = [x["name"] for x in dml("repo", "list")]
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        f"Branches({branch})": {x: url_for("branch_page", repo=repo, branch=x) for x in dml("branch", "list")},
        "Dag": {x["name"]: url_for("dag_page", repo=repo, branch=branch, dag_id=x["id"]) for x in dml("dag", "list")},
    }
    return render_template("index.html", dropdowns=dropdowns)


@app.route("/repo:<repo>/branch:<branch>/dag:<dag_id>")
def dag_page(repo, branch, dag_id):
    dml = Dml(repo=repo, branch=branch)
    repos = [x["name"] for x in dml("repo", "list")]
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        f"Branches({branch})": {x: url_for("branch_page", repo=repo, branch=x) for x in dml("branch", "list")},
        f"Dag({dag_id})": {
            x["name"]: url_for("dag_page", repo=repo, branch=branch, dag_id=x["id"]) for x in dml("dag", "list")
        },
    }
    try:
        dag_data = dml("dag", "describe", dag_id)
    except Exception:
        logger.exception("cannot graph dag")
        abort(404, f"problem graphing dag: {dag_id}")
    script = None
    for node in dag_data["nodes"]:
        node["link"] = url_for(
            "node_page",
            repo=repo,
            branch=branch,
            dag_id=dag_id,
            node_id=node["id"] or "",
        )
        if node["node_type"] in ["import", "fn"]:
            if node["node_type"] == "fn":
                sublist = [
                    [x["arg"], x["source"]]
                    for x in dag_data["edges"]
                    if x["type"] == "node" and x["target"] == node["id"]
                ]
                sublist = sorted(sublist, key=lambda x: x[0])
                node["sublist"] = [
                    [
                        b,
                        url_for(
                            "node_page",
                            repo=repo,
                            branch=branch,
                            dag_id=dag_id,
                            node_id=b,
                        ),
                    ]
                    for a, b in sublist
                ]
            (tgt_dag,) = [x["target"] for x in dag_data["edges"] if x["type"] == "dag" and x["source"] == node["id"]]
            node["parent"] = tgt_dag
            node["parent_link"] = url_for("dag_page", repo=repo, branch=branch, dag_id=tgt_dag)
    if dag_data.get("argv"):
        node = dml.get_node_value(Ref("node/" + dag_data["argv"]))
        if "script" in (node[0].data or {}):
            script = node[0].data["script"]
    logs = dag_data["logs"]
    if logs is not None:
        s3 = S3Store()
        logs = tree_map(lambda x: isinstance(x, str), lambda x: s3.get(x).decode(), logs)
    html_uri = None
    try:
        dag = dml.load(dag_id)
    except Exception:
        abort(404, f"no such dag (node page): {dag_id}")
    s3 = S3Store()
    val = None
    if dag_data["result"]:
        val = dag[dag_data["result"]].value()
        if isinstance(val, Resource) and re.match(r"^s3://.*\.html$", val.uri) and s3.exists(val):
            bucket, key = s3.parse_uri(val)
            html_uri = s3.client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket,
                    "Key": key,
                    "ResponseContentDisposition": "inline",
                    "ResponseContentType": "text/html",
                },
                ExpiresIn=3600,  # URL expires in 1 hour
            )
        val = pformat(val, depth=3)
    return render_template(
        "dag.html",
        dropdowns=dropdowns,
        data=dag_data,
        script=script,
        logs=logs,
        html_uri=html_uri,
        error=dag_data["error"],
        result=val,
    )


@app.route("/repo:<repo>/branch:<branch>/dag:<dag_id>/node:<node_id>")
def node_page(repo, branch, dag_id, node_id):
    dml = Dml(repo=repo, branch=branch)
    repos = [x["name"] for x in dml("repo", "list")]
    dropdowns = {
        f"Repos({repo})": {x: url_for("repo_page", repo=x) for x in repos},
        f"Branches({branch})": {x: url_for("branch_page", repo=repo, branch=x) for x in dml("branch", "list")},
        f"Dag({dag_id})": {
            x["name"]: url_for("dag_page", repo=repo, branch=branch, dag_id=x["id"]) for x in dml("dag", "list")
        },
    }
    try:
        dag = dml.load(dag_id)
    except Exception:
        abort(404, f"no such dag (node page): {dag_id}")
    s3 = S3Store()
    val = dag[node_id].value()
    html_uri = script = None
    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], Resource):
        script = (val[0].data or {}).get("script")
    if isinstance(val, Resource):
        script = (val.data or {}).get("script")
        if re.match(r"^s3://.*\.html$", val.uri) and s3.exists(val):
            bucket, key = s3.parse_uri(val)
            html_uri = s3.client.generate_presigned_url(
                "get_object",
                Params={
                    "Bucket": bucket,
                    "Key": key,
                    "ResponseContentDisposition": "inline",
                    "ResponseContentType": "text/html",
                },
                ExpiresIn=3600,  # URL expires in 1 hour
            )
    return render_template(
        "node.html",
        dropdowns=dropdowns,
        dag_id=dag_id,
        dag_link=url_for("dag_page", repo=repo, branch=branch, dag_id=dag_id),
        node_id=node_id,
        code=pformat(val, depth=3),
        html_uri=html_uri,
        script=script,
    )


def run():
    parser = ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=5000)
    args = parser.parse_args()
    app.run(debug=True, port=args.port)
