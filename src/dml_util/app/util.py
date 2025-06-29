import re
from pprint import pformat

from daggerml import Error, Resource
from daggerml.core import Ref

from dml_util.baseutil import S3Store


def get_sub(resource):
    while (sub := (resource.data or {}).get("sub")) is not None:
        resource = sub
    return resource


def get_node_repr(dag, node_id):
    val = dag[node_id].value()
    stack_trace = html_uri = script = None
    if isinstance(val, Error):
        try:
            stack_trace = "\n".join([x.strip() for x in val.context["trace"] if x.strip()])
        except Exception:
            pass
    elif isinstance(val, list) and len(val) > 0 and isinstance(val[0], Resource):
        script = (get_sub(val[0]).data or {}).get("script")
    elif isinstance(val, Resource):
        script = (get_sub(val).data or {}).get("script")
        s3 = S3Store()
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
    return {
        "script": script,
        "html_uri": html_uri,
        "stack_trace": stack_trace,
        "value": pformat(val, depth=3),
    }


def get_dag_info(dml, dag_id):
    out = {"dag_data": dml("dag", "describe", dag_id)}
    dag_data = out["dag_data"]
    for node in dag_data["nodes"]:
        if node["node_type"] in ["import", "fn"]:
            if node["node_type"] == "fn":
                node["sublist"] = [
                    x["source"] for x in dag_data["edges"] if x["type"] == "node" and x["target"] == node["id"]
                ]
            (tgt_dag,) = [x["target"] for x in dag_data["edges"] if x["type"] == "dag" and x["source"] == node["id"]]
            node["parent"] = tgt_dag
    if dag_data.get("argv"):
        node = dml.get_node_value(Ref(dag_data["argv"]))
        out["script"] = (get_sub(node[0]).data or {}).get("script")
    dag = dml.load(dag_id)
    for key in ["result", "argv"]:
        if dag_data.get(key) is not None:
            out[key] = get_node_repr(dag, dag_data[key])
    return out


def get_node_info(dml, dag_id, node_id):
    return get_node_repr(dml.load(dag_id), node_id)
