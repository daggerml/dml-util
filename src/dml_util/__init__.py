try:
    from dml_util.common import funkify
    from dml_util.executor import CFN_EXEC, DOCKER_EXEC, SCRIPT_EXEC, SSH_EXEC
    from dml_util.funk import dkr_build, dkr_push, query_update
except ModuleNotFoundError:
    pass

from dml_util.baseutil import S3Store

try:
    from dml_util.__about__ import __version__
except ImportError:
    __version__ = "local"
