try:
    import dml_util.runner  # to `register`
    import dml_util.wrapper  # to `new_dag` patch
    from dml_util.funk import aws_fndag, dkr_build, funkify
except ModuleNotFoundError:
    pass

from dml_util.baseutil import S3Store, dict_product

try:
    from dml_util.__about__ import __version__
except ImportError:
    __version__ = "local"
