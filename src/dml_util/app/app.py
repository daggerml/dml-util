import json
import os
import subprocess
from pprint import pprint

from daggerml import Dml, Resource
from flask import Flask, abort, render_template, request, send_from_directory, url_for

from dml_util.baseutil import S3_BUCKET, S3_PREFIX

app = Flask(__name__)

# Define the root directory that contains all commit subdirectories
ROOT_DIR = os.path.join(os.getcwd(), "root")
dml = Dml()


@app.route("/")
def index():
    dags = [x["name"] for x in dml("dag", "list")]
    return render_template("index.html", commits=dags)


@app.route("/dag/<dag_id>")
def dag_page(dag_id):
    dags = [x["name"] for x in dml("dag", "list")]
    dag_data = dml("dag", "graph", "--output", "json", dag_id)
    return render_template("dag.html", data=json.dumps(dag_data), commits=dags)


@app.route("/commit/<commit_name>/file/<filename>")
def serve_file(commit_name, filename):
    commit_dir = os.path.join(ROOT_DIR, commit_name)
    if not os.path.isdir(commit_dir):
        abort(404, description="Commit not found")
    file_path = os.path.join(commit_dir, filename)
    if not os.path.isfile(file_path):
        abort(404, description="File not found")
    return send_from_directory(commit_dir, filename)


if __name__ == "__main__":
    app.run(debug=True)
