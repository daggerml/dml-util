try:
    import dml_util.runner  # to `register`
    from dml_util.funk import aws_fndag, dkr_build, dkr_push, funkify
except ModuleNotFoundError:
    pass

from dml_util.baseutil import S3_BUCKET, S3_PREFIX, S3Store, dict_product

try:
    from dml_util.__about__ import __version__
except ImportError:
    __version__ = "local"
