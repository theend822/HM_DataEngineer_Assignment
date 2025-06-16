from airflow.models import DagRun
from datetime import timedelta

# execution_date_fn expects expects a function that takes exactly one argument (dt) and returns a datetime. therefore, to pass 2 arguments, we need to wrap it up.
def get_most_recent_dag_run(dag_id):
    def _get_execution_date(dt):
        from airflow import settings
        
        session = settings.Session()
        try:
            # Look for any successful run in the last 1 days
            cutoff_date = dt - timedelta(days=1)
            
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.state == 'success',
                DagRun.execution_date >= cutoff_date
            ).order_by(DagRun.execution_date.desc()).first()
            
            return dag_run.execution_date if dag_run else dt
        finally:
            session.close()
    
    return _get_execution_date 