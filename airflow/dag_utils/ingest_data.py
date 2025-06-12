from dag_utils.PostgresManager import PostgresManager

def ingest_data(file_path, table_name):
    pg = PostgresManager()
    return pg.ingest_csv(file_path, table_name)