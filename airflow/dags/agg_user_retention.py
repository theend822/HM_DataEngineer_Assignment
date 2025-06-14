from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timezone

with DAG(
    "agg_user_retention",
    start_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_for_fct_events = ExternalTaskSensor(
        task_id='wait_for_fct_events',
        external_dag_id='fct_events',
        external_task_id=None,  # Wait for entire DAG
        timeout=600,
        poke_interval=30
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt deps",
    )

    dbt_build_agg_user_retention = BashOperator(
        task_id="dbt_build_agg_user_retention",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt run --models agg_user_retention",
    )

    dbt_test_agg_user_retention = BashOperator(
        task_id="dbt_test_agg_user_retention",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt test --models agg_user_retention",
    )

    wait_for_fct_events >> dbt_deps >> dbt_build_agg_user_retention >> dbt_test_agg_user_retention
