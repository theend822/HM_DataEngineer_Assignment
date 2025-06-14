from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timezone

with DAG(
    "dim_users",
    start_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag:

    wait_for_fct_event_stream = ExternalTaskSensor(
        task_id='wait_for_fct_event_stream',
        external_dag_id='fct_event_stream',
        external_task_id='load_fct',
        timeout=600,
        poke_interval=30
    )

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt deps",
    )

    dbt_test_upstream = BashOperator(
        task_id="dbt_test_upstream_data",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt test --models test:dim_users_no_multi_attr",
    )

    dbt_build_dim_users = BashOperator(
        task_id="dbt_build_dim_users",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt run --models dim_users",
    )

    dbt_test_dim_users = BashOperator(
        task_id="dbt_test_dim_users",
        bash_command="docker exec heymax_loyalty-dbt-1 dbt test --models dim_users",
    )

    wait_for_fct_event_stream >> dbt_deps >> dbt_test_upstream >> dbt_build_dim_users >> dbt_test_dim_users