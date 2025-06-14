from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timezone

with DAG(
    "agg_active_user",
    start_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_for_dim_users = ExternalTaskSensor(
        task_id='wait_for_dim_users',
        external_dag_id='dim_users',
        external_task_id=None,  # Wait for entire DAG
        timeout=600,
        poke_interval=30
    )

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

    dbt_build_agg_active_user = BashOperator(
        task_id="dbt_build_agg_active_user",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt run --models agg_active_user",
    )

    dbt_test_agg_active_user = BashOperator(
        task_id="dbt_test_agg_active_user",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt test --models agg_active_user",
    )

    [wait_for_dim_users, wait_for_fct_events] >> dbt_deps >> dbt_build_agg_active_user >> dbt_test_agg_active_user