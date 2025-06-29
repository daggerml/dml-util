import logging
import subprocess
from functools import wraps

import daggerml

logger = logging.getLogger(__name__)
_old_dml_new = daggerml.Dml.new


@wraps(daggerml.Dml.new)
def new_dag(cls, *args, **kwargs):
    """Create a new DAG within the provided Dml instance."""
    logger.info("Creating new DAG with args: %s, kwargs: %s", args, kwargs)
    logger.info("Using Dml instance: %s", cls)
    dag = _old_dml_new(cls, *args, **kwargs)

    def cli(*args):
        try:
            return subprocess.check_output(["git", *args]).strip().decode("utf-8")
        except (KeyboardInterrupt, SystemExit):
            raise
        except Exception as e:
            logger.error("Error running git command: %s", e)

    git_info = {
        "branch": cli("rev-parse", "--abbrev-ref", "HEAD"),
        "remote": cli("config", "--get", "remote.origin.url"),
        "commit": cli("rev-parse", "HEAD"),
        "status": cli("status", "--porcelain"),
    }
    dag[".dml/git"] = {k: v for k, v in git_info.items() if v is not None}
    return dag


daggerml.Dml.new = new_dag
