try:
    import dml_util.monkey_patch
    from dml_util.common import DOCKER_EXEC, SCRIPT_EXEC, funkify
    from dml_util.dags import CFN_EXEC
    from dml_util.funk import dkr_build, dkr_push, query_update
except ModuleNotFoundError:
    pass

from dml_util.baseutil import DagRunError, Runner, S3Store

try:
    from dml_util.__about__ import __version__
except ImportError:
    __version__ = "local"
