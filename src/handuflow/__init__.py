"""
HanduFlow — Spark + Delta Lake data movement orchestration.

Quick start::

    from handuflow import run

    result = run(spark, config_path="/path/to/handuflow_dir/config.ini")
"""

from handuflow.api import *  # noqa: F403
from handuflow.api import __all__, __version__
