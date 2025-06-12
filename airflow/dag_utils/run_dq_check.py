from dag_utils.PostgresManager import PostgresManager

def run_dq_check(check_name, sql_query):
    pg = PostgresManager()
    return pg.run_dq_check(check_name, sql_query)