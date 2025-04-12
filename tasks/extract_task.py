from airflow.decorators import task
import pandas as pd
import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "database": "lung_cancer_database",
    "user": "postgres",
    "password": "admin"
}

@task()
def extract_from_postgres():
    """Extrae datos desde la tabla lung_cancer_post en PostgreSQL y los retorna como dict"""
    conn = psycopg2.connect(**DB_PARAMS)
    df = pd.read_sql("SELECT * FROM lung_cancer_post", conn)
    conn.close()
    return df.to_dict()
