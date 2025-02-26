from dml_util import funkify


@funkify
def query_update(dag):
    from dml_util.common import update_query

    old_rsrc, params = dag.argv[1:].value()
    dag.result = update_query(old_rsrc, params)


@funkify
def dkr_build(dag):
    from dml_util.lib.dkr import dkr_build

    tarball = dag.argv[1].value()
    flags = dag.argv[2].value() if len(dag.argv) > 2 else []
    dag.info = dkr_build(tarball.uri, flags)
    dag.result = dag.info["image"]


@funkify
def dkr_push(dag):
    from daggerml import Resource

    from dml_util.lib.dkr import dkr_push

    image = dag.argv[1].value()
    repo = dag.argv[2].value()
    if isinstance(repo, Resource):
        repo = repo.uri
    dag.info = dkr_push(image, repo)
    dag.result = dag.info["image"]
