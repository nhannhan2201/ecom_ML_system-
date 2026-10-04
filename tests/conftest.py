"""
Pytest configuration and global fixtures for ecom_ML_system.
"""

import sys
import os
import types
import pytest

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
if os.path.join(PROJECT_ROOT, "governance") not in sys.path:
    sys.path.insert(0, os.path.join(PROJECT_ROOT, "governance"))

# Setup mock airflow if running in Python 3.13 or environment where apache-airflow is not installed
if "airflow" not in sys.modules:
    try:
        import airflow  # noqa: F401
    except ImportError:
        af = types.ModuleType("airflow")
        af_models = types.ModuleType("airflow.models")
        af_ops = types.ModuleType("airflow.operators")
        af_bash = types.ModuleType("airflow.operators.bash")
        af_trig = types.ModuleType("airflow.operators.trigger_dagrun")
        af_hooks = types.ModuleType("airflow.hooks")
        af_hooks_base = types.ModuleType("airflow.hooks.base")

        class MockDAG:
            current_dag = None

            def __init__(self, dag_id, description=None, schedule_interval=None, start_date=None, catchup=False, tags=None, default_args=None):
                self.dag_id = dag_id
                self.description = description
                self.schedule_interval = schedule_interval
                self.start_date = start_date
                self.catchup = catchup
                self.tags = tags or []
                self.default_args = default_args or {}
                self.tasks = {}

            def __enter__(self):
                MockDAG.current_dag = self
                return self

            def __exit__(self, *args):
                MockDAG.current_dag = None

        class MockBaseOperator:
            def __init__(self, task_id, **kwargs):
                self.task_id = task_id
                self.downstream_list = []
                self.upstream_list = []
                self.kwargs = kwargs
                for k, v in kwargs.items():
                    setattr(self, k, v)
                if MockDAG.current_dag:
                    MockDAG.current_dag.tasks[task_id] = self

            def __rshift__(self, other):
                if isinstance(other, MockBaseOperator):
                    self.downstream_list.append(other)
                    other.upstream_list.append(self)
                return other

            def __lshift__(self, other):
                if isinstance(other, MockBaseOperator):
                    self.upstream_list.append(other)
                    other.downstream_list.append(self)
                return other

        class MockVariable:
            @classmethod
            def get(cls, key, default_var=None):
                return default_var

        class MockBaseHook:
            @classmethod
            def get_connection(cls, conn_id):
                raise Exception("MockBaseHook: no real airflow database configured")

        af.DAG = MockDAG
        af_models.Variable = MockVariable
        af_bash.BashOperator = MockBaseOperator
        af_trig.TriggerDagRunOperator = MockBaseOperator
        af_hooks_base.BaseHook = MockBaseHook

        sys.modules["airflow"] = af
        sys.modules["airflow.models"] = af_models
        sys.modules["airflow.operators"] = af_ops
        sys.modules["airflow.operators.bash"] = af_bash
        sys.modules["airflow.operators.trigger_dagrun"] = af_trig
        sys.modules["airflow.hooks"] = af_hooks
        sys.modules["airflow.hooks.base"] = af_hooks_base


@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    """Set standardized mock environment variables for tests."""
    monkeypatch.setenv("MINIO_ENDPOINT", "http://localhost:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "minioadmin")
    monkeypatch.setenv("MINIO_SECRET_KEY", "minioadmin")
    monkeypatch.setenv("DATAHUB_GMS_URL", "http://localhost:8089")
