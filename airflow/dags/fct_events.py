from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timezone
from dag_utils.lastest_dag_run import get_most_recent_dag_run

with DAG(
    "fct_events",
    start_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_for_fct_event_stream = ExternalTaskSensor(
        task_id='wait_for_fct_event_stream',
        external_dag_id='fct_event_stream',
        external_task_id=None,
        execution_date_fn=get_most_recent_dag_run('fct_event_stream'),
        timeout=600,
        poke_interval=30
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="docker exec heymax_assignment-dbt-1 dbt deps",
    )

    dbt_build_fct_events = BashOperator(
        task_id="dbt_build_fct_events",
        bash_command="docker exec heymax_assignment-dbt-1 dbt run --models fct_events",
    )

    dbt_test_fct_events = BashOperator(
        task_id="dbt_test_fct_events",
        bash_command="docker exec heymax_assignment-dbt-1 dbt test --models fct_events",
    )

    wait_for_fct_event_stream >> dbt_deps >> dbt_build_fct_events >> dbt_test_fct_events
