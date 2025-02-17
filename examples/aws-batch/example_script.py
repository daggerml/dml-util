def main():
    # wrapped in a function so an importing tool won't auto execute it
    try:
        import subprocess

        subprocess.check_call(["pip", "install", "boto3", "dml-util==0.0.2"])
    except Exception as e:
        print("ruh roh! can't install libs!", e)
        raise
    import os
    from urllib.parse import urlparse

    import boto3
    from daggerml import Dml

    INPUT_LOC = os.environ["DML_INPUT_LOC"]
    OUTPUT_LOC = os.environ["DML_OUTPUT_LOC"]

    def handler(dump):
        p = urlparse(OUTPUT_LOC)
        boto3.client("s3").put_object(Bucket=p.netloc, Key=p.path[1:], Body=dump.encode())

    p = urlparse(INPUT_LOC)
    data = boto3.client("s3").get_object(Bucket=p.netloc, Key=p.path[1:])["Body"].read().decode()
    with Dml(data=data, message_handler=handler) as dml:
        with dml.new("test", "test") as d0:
            d0.n0 = sum(d0.argv[1:].value())
            d0.result = d0.n0


if __name__ == "__main__":
    main()
