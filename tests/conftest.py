"""Shared pytest fixtures: local SparkSession + per-task module loader.

Every task in this repo ships a `transformations.py`. Python can only hold one
module named `transformations` in `sys.modules` at a time, so we load each
task's module under a unique synthetic name (e.g. `transformations_geocode`)
via `importlib.util.spec_from_file_location`. Tests import by calling
`load_task_module(<task_path>)` rather than by path manipulation.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent


def load_task_module(task_relpath: str, synthetic_name: str) -> ModuleType:
    """Load a task's transformations.py under a unique name.

    `task_relpath` is relative to the repo root, e.g.
    `pipelines/flood/etl/geocode_addresses`. `synthetic_name` must be unique
    across the test suite so modules don't collide in `sys.modules`.
    """
    task_dir = REPO_ROOT / task_relpath
    source = task_dir / "transformations.py"
    if not source.exists():
        raise FileNotFoundError(source)

    # Inject the task dir at the front of sys.path so any sibling-module
    # imports inside transformations.py resolve against its own folder.
    task_dir_str = str(task_dir)
    if task_dir_str not in sys.path:
        sys.path.insert(0, task_dir_str)

    spec = importlib.util.spec_from_file_location(synthetic_name, source)
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not build spec for {source}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[synthetic_name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession shared across every test in the repo."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("absa-ubunye-pipelines-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .getOrCreate()
    )
    yield session
    session.stop()
