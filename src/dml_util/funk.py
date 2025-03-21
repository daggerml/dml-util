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
    return


@funkify
def execute_notebook(dag):
    import subprocess
    import sys
    from tempfile import TemporaryDirectory

    from dml_util import S3Store

    def run(*cmd, check=True, **kwargs):
        resp = subprocess.run(cmd, check=False, text=True, capture_output=True, **kwargs)
        if resp.returncode == 0:
            print(resp.stderr, file=sys.stderr)
            return resp.stdout.strip()
        msg = f"STDOUT:\n{resp.stdout}\n\n\nSTDERR:\n{resp.stderr}"
        print(msg)
        if check:
            raise RuntimeError(msg)

    s3 = S3Store()
    with TemporaryDirectory() as tmpd:
        with open(f"{tmpd}/nb.ipynb", "wb") as f:
            f.write(s3.get(dag.argv[1]))
        jupyter = run("which", "jupyter", check=True)
        run(
            jupyter,
            "nbconvert",
            "--execute",
            "--to=notebook",
            "--output=foo",
            f"--output-dir={tmpd}",
            f"{tmpd}/nb.ipynb",
        )
        dag.ipynb = s3.put(filepath=f"{tmpd}/foo.ipynb", suffix=".ipynb")
        run(
            jupyter,
            "nbconvert",
            "--to=html",
            f"--output-dir={tmpd}",
            f"{tmpd}/foo.ipynb",
        )
        dag.html = s3.put(filepath=f"{tmpd}/foo.html", suffix=".html")
    dag.result = dag.html
