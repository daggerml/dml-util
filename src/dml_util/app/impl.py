import logging
from argparse import ArgumentParser

from daggerml import Dml
from flask import Flask, jsonify, render_template, request, url_for

from dml_util.app.util import get_dag_info, get_node_info

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def get_dropdowns(dml, repo, branch, dag_id):
    dropdowns = {"repos": {x["name"]: url_for("main", repo=x["name"]) for x in dml("repo", "list")}}
    if repo is not None:
        dropdowns["branches"] = {x: url_for("main", repo=repo, branch=x) for x in dml("branch", "list")}
    if branch is not None:
        tmp = {x["name"]: x["id"] for x in dml("dag", "list")}
        dropdowns["dags"] = {k: url_for("main", repo=repo, branch=branch, dag_id=v) for k, v in tmp.items()}
    return dropdowns


def create_app(dml=None):
    app = Flask(__name__)

    @app.route("/")
    def main():
        repo = request.args.get("repo")
        branch = request.args.get("branch")
        dag_id = request.args.get("dag_id")
        node_id = request.args.get("node_id")
        dropdowns = get_dropdowns(dml, repo, branch, dag_id)
        if dag_id is None:
            return render_template("index.html", dropdowns=dropdowns)
        if node_id is None:
            print("running dag_id", dag_id)
            data = get_dag_info(dml, dag_id)
            data.pop("argv", None)
            data.update(data.pop("result", {}))
            dag_data = data.pop("dag_data")
            for node in dag_data["nodes"]:
                node["link"] = url_for(
                    "main",
                    repo=repo,
                    branch=branch,
                    dag_id=dag_id,
                    node_id=node["id"] or "",
                )
                if node["node_type"] in ["import", "fn"]:
                    node["parent_link"] = url_for("main", repo=repo, branch=branch, dag_id=node["parent"])
                    if node["node_type"] == "fn":
                        node["sublist"] = [
                            [
                                x,
                                url_for(
                                    "main",
                                    repo=repo,
                                    branch=branch,
                                    dag_id=dag_id,
                                    node_id=x,
                                ),
                            ]
                            for x in node["sublist"]
                        ]
            return render_template("dag.html", dropdowns=dropdowns, data=dag_data, **data)
        data = get_node_info(dml, dag_id, node_id)
        return render_template(
            "node.html",
            dropdowns=dropdowns,
            dag_id=dag_id,
            dag_link=url_for("main", repo=repo, branch=branch, dag_id=dag_id),
            node_id=node_id,
            **data,
        )

    @app.route("/idx")
    def idx():
        repo = request.args.get("repo")
        branch = request.args.get("branch")
        dropdowns = get_dropdowns(dml, repo, branch, None)
        return render_template("indexes.html", dropdowns=dropdowns)

    @app.route("/kill-indexes", methods=["POST"])
    def kill_idx():
        repo = request.args.get("repo")
        branch = request.args.get("branch")
        body = request.form.getlist("del-idx", type=str)
        for idx in body:
            dml("index", "delete", idx)
        idxs = dml("index", "list")
        for idx in idxs:
            idx["dag_link"] = url_for("main", repo=repo, branch=branch, dag_id=idx["dag"])
        return jsonify({"deleted": len(body), "indexes": idxs})

    return app


def run():
    parser = ArgumentParser()
    parser.add_argument("-p", "--port", type=int, default=5000)
    args = parser.parse_args()
    create_app(Dml()).run(debug=True, port=args.port)
