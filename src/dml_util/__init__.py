from dml_util.common import CFN_EXEC, DOCKER_EXEC, SCRIPT_EXEC, funkify
from dml_util.funk import dkr_build, dkr_push, query_update
from dml_util.ingest import S3

try:
    from dml_util.__about__ import __version__
except ImportError:
    __version__ = "local"
