def main():
    # wrapped in a function so an importing tool won't auto execute it
    try:
        import subprocess

        subprocess.check_call(["pip", "install", "boto3", "dml-util[dml]==0.0.6"])
    except Exception as e:
        print("ruh roh! can't install libs!", e)
        raise
    from dml_util.executor import aws_fndag

    with aws_fndag() as dag:
        dag.n0 = sum(dag.argv[1:].value())
        dag.result = dag.n0


if __name__ == "__main__":
    main()
