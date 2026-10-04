"""
Test suite for Airflow DAGs importability, structure, and trigger chaining.
Validates:
  - 0 import errors across all 4 DAGs (DP1, DP2, DP3, DP4)
  - Correct task definitions and expected task IDs
  - Ingest >> Validate >> Trigger pipeline sequence
  - Downstream trigger cascade: DP1 -> DP2 -> DP3 -> DP4
"""



def test_dags_import_and_structure():
    """Verify that all 4 Airflow DAGs import without errors and have expected configurations."""
    import dags.dp1_raw_to_bronze as dp1_module
    import dags.dp2_bronze_to_silver_and_gold as dp2_module
    import dags.dp3_compute_offline_features as dp3_module
    import dags.dp4_feast_materialize as dp4_module

    # 1. Assert DAG IDs
    assert dp1_module.dag.dag_id == "dp1_raw_to_bronze"
    assert dp2_module.dag.dag_id == "dp2_bronze_to_silver_and_gold"
    assert dp3_module.dag.dag_id == "dp3_compute_offline_features"
    assert dp4_module.dag.dag_id == "dp4_feast_materialize"


def test_dp1_dag_tasks_and_dependencies():
    """Verify DP1 tasks, execution sequence, and downstream trigger."""
    import dags.dp1_raw_to_bronze as dp1_module
    tasks = dp1_module.dag.tasks

    expected_tasks = {"ingest_stage", "validate_stage", "trigger_dp2_pipeline"}
    assert set(tasks.keys()) == expected_tasks

    # Verify operator types and commands
    assert "spark_optimized.py --stage dp1 --step ingest" in tasks["ingest_stage"].bash_command
    assert "spark_optimized.py --stage dp1 --step validate" in tasks["validate_stage"].bash_command

    # Verify trigger target
    assert tasks["trigger_dp2_pipeline"].trigger_dag_id == "dp2_bronze_to_silver_and_gold"

    # Verify dependency chain
    assert tasks["validate_stage"] in tasks["ingest_stage"].downstream_list
    assert tasks["trigger_dp2_pipeline"] in tasks["validate_stage"].downstream_list


def test_dp2_dag_tasks_and_dependencies():
    """Verify DP2 tasks, execution sequence, and downstream trigger."""
    import dags.dp2_bronze_to_silver_and_gold as dp2_module
    tasks = dp2_module.dag.tasks

    expected_tasks = {"ingest_stage", "validate_stage", "trigger_dp3_pipeline"}
    assert set(tasks.keys()) == expected_tasks

    assert "spark_optimized.py --stage dp2 --step ingest" in tasks["ingest_stage"].bash_command
    assert "spark_optimized.py --stage dp2 --step validate" in tasks["validate_stage"].bash_command
    assert tasks["trigger_dp3_pipeline"].trigger_dag_id == "dp3_compute_offline_features"

    assert tasks["validate_stage"] in tasks["ingest_stage"].downstream_list
    assert tasks["trigger_dp3_pipeline"] in tasks["validate_stage"].downstream_list


def test_dp3_dag_tasks_and_dependencies():
    """Verify DP3 tasks, execution sequence, and downstream trigger."""
    import dags.dp3_compute_offline_features as dp3_module
    tasks = dp3_module.dag.tasks

    expected_tasks = {"ingest_stage", "validate_stage", "trigger_dp4_pipeline"}
    assert set(tasks.keys()) == expected_tasks

    assert "spark_optimized.py --stage dp3 --step ingest" in tasks["ingest_stage"].bash_command
    assert "spark_optimized.py --stage dp3 --step validate" in tasks["validate_stage"].bash_command
    assert tasks["trigger_dp4_pipeline"].trigger_dag_id == "dp4_feast_materialize"

    assert tasks["validate_stage"] in tasks["ingest_stage"].downstream_list
    assert tasks["trigger_dp4_pipeline"] in tasks["validate_stage"].downstream_list


def test_dp4_dag_tasks_and_dependencies():
    """Verify DP4 tasks and execution sequence."""
    import dags.dp4_feast_materialize as dp4_module
    tasks = dp4_module.dag.tasks

    expected_tasks = {"task_validate_gold_source", "task_feast_incremental_materialize"}
    assert set(tasks.keys()) == expected_tasks

    assert "materialize.py --mode incremental" in tasks["task_feast_incremental_materialize"].bash_command
    assert tasks["task_feast_incremental_materialize"] in tasks["task_validate_gold_source"].downstream_list


def test_trigger_cascade_continuity():
    """Verify end-to-end trigger cascade continuity across all 4 pipelines: DP1 -> DP2 -> DP3 -> DP4."""
    import dags.dp1_raw_to_bronze as dp1_module
    import dags.dp2_bronze_to_silver_and_gold as dp2_module
    import dags.dp3_compute_offline_features as dp3_module
    import dags.dp4_feast_materialize as dp4_module

    # DP1 triggers DP2
    dp1_target = dp1_module.dag.tasks["trigger_dp2_pipeline"].trigger_dag_id
    assert dp1_target == dp2_module.dag.dag_id

    # DP2 triggers DP3
    dp2_target = dp2_module.dag.tasks["trigger_dp3_pipeline"].trigger_dag_id
    assert dp2_target == dp3_module.dag.dag_id

    # DP3 triggers DP4
    dp3_target = dp3_module.dag.tasks["trigger_dp4_pipeline"].trigger_dag_id
    assert dp3_target == dp4_module.dag.dag_id
