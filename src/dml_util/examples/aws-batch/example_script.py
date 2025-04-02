if __name__ == "__main__":
    try:
        import subprocess

        subprocess.check_call(["pip", "install", "boto3", "dml-util[dml]"])
    except Exception as e:
        print("ruh roh! can't install libs!", e)
        raise
    from dml_util.adapter import aws_fndag

    with aws_fndag() as dag:
        dag.n0 = sum(dag.argv[1:].value())
        dag.result = dag.n0
