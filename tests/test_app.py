from daggerml import Dml, Error, Resource

from dml_util import S3Store, funkify
from dml_util.app.impl import create_app
from dml_util.app.util import get_dag_info, get_sub
from tests.test_all import FullDmlTestCase


class TestAppUtil(FullDmlTestCase):
    def test_get_sub(self):
        resource = Resource("uri0", data={"sub": Resource("uri1", data={"sub": Resource("uri2")})})
        assert get_sub(resource) == Resource("uri2")

    def test_funkify(self):
        def fn(*args):
            return sum(args)

        @funkify(extra_fns=[fn])
        def dag_fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            dag.result = fn(*dag.argv[1:].value())
            return dag.result

        def assert_info(dml, info, logs=None):
            dag_data = info["dag_data"]
            if logs is not None:
                s3 = S3Store()
                dag_data["logs"] = {k: s3.get(v).decode().strip() for k, v in dag_data["logs"].items()}
            assert dag_data["logs"] == logs
            # assert the sublist (the fn args in order)
            for node in dag_data["nodes"]:
                assert {"data_type", "doc", "id", "name", "node_type"} <= set(node.keys())
                if node["node_type"] in ["import", "fn"]:
                    assert "parent" in node
                    if node["node_type"] == "fn":
                        assert "sublist" in node
                        assert set(node["sublist"]) <= set([x["id"] for x in dag_data["nodes"]])

        with Dml() as dml:
            assert len(dml("dag", "list", "--all")) == 0
            vals = [1, 2, 3]
            with dml.new("d0", "d0") as dag:
                f0 = dag._put(dag_fn, name="f0")
                n0 = f0(*vals, name="d0")
                dag.result = n0
            assert dag.result.value() == sum(vals)
            assert len(dml("dag", "list")) == 1
            assert len(dml("dag", "list", "--all")) > 1
            info = get_dag_info(dml, dag._ref.to)
            assert_info(dml, info, logs=None)
            assert info == get_dag_info(dml, "d0")
            (fndag_id,) = [
                x["parent"] for x in info["dag_data"]["nodes"] if x["node_type"] == "fn" and len(x["sublist"]) == 4
            ]
            assert fndag_id in [x["id"] for x in dml("dag", "list", "--all")]
            info = get_dag_info(dml, fndag_id)
            assert_info(dml, info, logs={x: f"testing {x}..." for x in ["stdout", "stderr"]})


class TestAppWeb(FullDmlTestCase):
    def test_page_root(self):
        with Dml() as dml:
            client = create_app(dml).test_client()
            response = client.get("/")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                """<a class="dropdown-item" href="/?repo=test"> test </a>""",
                response.data.decode(),
            )

    def test_page_repo(self):
        with Dml() as dml:
            client = create_app(dml).test_client()
            response = client.get("/?repo=test")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                """<a class="dropdown-item" href="/?repo=test&amp;branch=main"> main </a>""",
                response.data.decode(),
            )

    def test_page_branch(self):
        with Dml() as dml:
            with dml.new("dag0", "this is a test") as dag:
                dag._commit(1)
            dag_id = dag._ref.to
            client = create_app(dml).test_client()
            response = client.get("/?repo=test&branch=main")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                f"""<a class="dropdown-item" href="/?repo=test&amp;branch=main&amp;dag_id={dag_id}"> dag0 </a>""",
                response.data.decode(),
            )

    def test_page_dag(self):
        with Dml() as dml:
            with dml.new("dag0", "this is a test") as dag:
                n0 = dag._put("asdf", name="n0")
                dag._commit(n0)
            dag_id = dag._ref.to
            client = create_app(dml).test_client()
            response = client.get(f"/?repo=test&branch=main&dag_id={dag_id}")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                f"""<a class="dropdown-item" href="/?repo=test&amp;branch=main&amp;dag_id={dag_id}"> dag0 </a>""",
                response.data.decode(),
            )

    def test_page_dag_w_err(self):
        with Dml() as dml:
            try:
                with dml.new("dag0", "this is a test") as dag:
                    _ = 1 / 0
            except ZeroDivisionError:
                pass
            dag_id = dag._ref.to
            client = create_app(dml).test_client()
            response = client.get(f"/?repo=test&branch=main&dag_id={dag_id}")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                f"""<a class="dropdown-item" href="/?repo=test&amp;branch=main&amp;dag_id={dag_id}"> dag0 </a>""",
                response.data.decode(),
            )

    def test_page_node(self):
        with Dml() as dml:
            with dml.new("dag0", "this is a test") as dag:
                n0 = dag._put("asdf", name="n0")
                dag._commit(n0)
            dag_id = dag._ref.to
            node_id = n0.ref.to
            client = create_app(dml).test_client()
            response = client.get(f"/?repo=test&branch=main&dag_id={dag_id}&node_id={node_id}")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                f"""<a class="dropdown-item" href="/?repo=test&amp;branch=main&amp;dag_id={dag_id}"> dag0 </a>""",
                response.data.decode(),
            )

    def test_page_node_w_err(self):
        @funkify
        def dag_fn(dag):
            import sys

            print("testing stdout...")
            print("testing stderr...", file=sys.stderr)
            print(1 / 0)

        with Dml() as dml:
            try:
                with dml.new("d0", "d0") as dag:
                    f0 = dag._put(dag_fn, name="f0")
                    n0 = f0(1, 0, name="d0")
                    dag.result = n0
            except Error:
                pass
            dag_id = dag._ref.to
            client = create_app(dml).test_client()
            response = client.get(f"/?repo=test&branch=main&dag_id={dag_id}")
            self.assertEqual(response.status_code, 200)
            self.assertIn(
                f"""<a class="dropdown-item" href="/?repo=test&amp;branch=main&amp;dag_id={dag_id}"> d0 </a>""",
                response.data.decode(),
            )
