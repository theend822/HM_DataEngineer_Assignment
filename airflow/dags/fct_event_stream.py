from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timezone
from dag_utils.ingest_data import ingest_data
from dag_utils.execute_sql import execute_sql
from dag_utils.run_dq_check import run_dq_check
from dag_utils.create_table import create_table
from dq_check.stg_event_stream import DQ_CHECKS

"""
================================================================================
RAW DATA PIPELINE: FCT_EVENT_STREAM
================================================================================

This pipeline processes raw loyalty program event data from CSV files into 
a structured PostgreSQL fact table, with comprehensive data quality validation.

PURPOSE:
• Ingest raw event data from CSV files into staging area
• Perform data quality checks on business-critical fields
• Transform and load clean data into fact table for downstream analytics

DATA FLOW:
Raw CSV → stg_event_stream → DQ Checks → fct_event_stream

PIPELINE STEPS:
1. CREATE STAGING TABLE: Creates stg_event_stream with VARCHAR columns for flexible ingestion
2. LOAD STAGING DATA: Ingests raw CSV data into staging table
3. DATA QUALITY CHECKS: Validates business rules across 3 categories:
   • NULL_CHECK: Ensures required fields are populated
   • ACCEPT_VALUE_CHECK: Validates enum values (event_type, platform, country, etc.)
   • FORMAT_CHECK: Validates data formats (timestamps, user_id patterns)
4. CREATE FACT TABLE: Creates fct_event_stream with proper data types
5. LOAD FACT TABLE: Transforms and loads validated data with type casting

DATA QUALITY CATEGORIES:
• NULL_CHECK: Validates required fields are not null
• ACCEPT_VALUE_CHECK: Ensures values match business enums
• FORMAT_CHECK: Validates regex patterns for timestamps and IDs

EVENT TYPES INCLUDED:
• miles_earned: User earned miles from transactions
• miles_redeemed: User spent miles on rewards  
• share: User shared content
• like: User liked content
• reward_search: User searched for rewards

BUSINESS RULES ENFORCED:
• transaction_category required only for transactional events (miles_earned, miles_redeemed, reward_search)
• miles_amount required only for monetary events (miles_earned, miles_redeemed)
• user_id must follow pattern: u_XXXX (4 digits)
• timestamp must be in format: YYYY-MM-DD HH:MM:SS.SSSSSS

================================================================================
"""

with DAG (
    "fct_event_stream",
    start_date=datetime(2025, 6, 1, tzinfo=timezone.utc),
    schedule_interval=None,
    catchup=False,
) as dag: 

    create_stg_table = PythonOperator(
        task_id='create_stg_table',
        python_callable=create_table,
        op_kwargs = {
            "table_schema":"/opt/airflow/table_schema/stg_event_stream.sql",
            "log_message":"Successfully created stage table stg_event_stream",
        },
    )

    load_stg = PythonOperator(
        task_id="load_stg",
        python_callable=ingest_data,
        op_kwargs={
            "file_path":"/opt/airflow/raw_data/event_stream.csv", 
            "table_name":"stg_event_stream",
        },
    )

    dq_tasks = []
    for check_type, check_content in DQ_CHECKS.items():
        for col_name, sql_query in check_content.items():
            dq_task = PythonOperator(
                task_id=f"dq_check_{check_type}_{col_name}",
                python_callable=run_dq_check,
                op_kwargs={
                    "check_name": f"{check_type}_{col_name}",
                    "sql_query": sql_query,
                },
            )
            dq_tasks.append(dq_task)

    create_fct_table = PythonOperator(
        task_id='create_fct_table',
        python_callable=create_table,
        op_kwargs = {
            "table_schema":"/opt/airflow/table_schema/fct_event_stream.sql",
            "log_message":"Successfully created fct table fct_event_stream",
        },
    )

    load_fct = PythonOperator(
        task_id="load_fct",
        python_callable=execute_sql,
        op_kwargs={
            "sql_query":"""
                INSERT INTO fct_event_stream (
                    event_time, user_id, event_type, transaction_category, 
                    miles_amount, platform, utm_source, country
                )
                SELECT 
                    event_time::TIMESTAMP,
                    user_id,
                    event_type,
                    transaction_category,
                    miles_amount::DECIMAL(10,2),
                    platform,
                    utm_source,
                    country
                FROM stg_event_stream
            """,
            "log_message":"Successfully loaded fct_event_stream table",
        },
    )

    create_stg_table >> load_stg >> dq_tasks >> create_fct_table >> load_fct