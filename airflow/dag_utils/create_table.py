from dag_utils.PostgresManager import PostgresManager

def create_table(table_schema, log_message):
    """
    Create database table from SQL schema file.

    Reads a .sql file containing table creation DDL statements and executes it
    using the PostgresManager.execute_sql method.
    """
    pg = PostgresManager()

    with open(table_schema, 'r') as file:
        sql_query = file.read()

    return pg.execute_sql(sql_query, log_message)