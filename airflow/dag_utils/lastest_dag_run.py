from airflow.models import DagRun
from datetime import timedelta

def get_most_recent_dag_run(dt):
    from airflow import settings
    
    session = settings.Session()
    try:
        # Look for any successful run in the last 7 days
        cutoff_date = dt - timedelta(days=7)
        
        dag_run = session.query(DagRun).filter(
            DagRun.dag_id == "fct_event_stream",
            DagRun.state == 'success',
            DagRun.execution_date >= cutoff_date
        ).order_by(DagRun.execution_date.desc()).first()
        
        return dag_run.execution_date if dag_run else dt
    finally:
        session.close()