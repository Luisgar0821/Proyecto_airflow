from airflow.decorators import dag
from datetime import datetime
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from tasks.extract_task import extract_from_postgres
from tasks.transform import transform_data
from tasks.load_dimensions import load_dimensions
from tasks.load_fact import load_fact_table

@dag(
    dag_id="etl_lung_cancer",
    start_date=datetime(2024, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["lung_cancer", "etl"]
)
def etl_lung_cancer_pipeline():
    """Pipeline ETL para cargar datos de cáncer de pulmón en modelo dimensional"""
    data = extract_from_postgres()
    cleaned = transform_data(data)
    load_dimensions(cleaned)
    load_fact_table(cleaned)

# Exponer el DAG
dag = etl_lung_cancer_pipeline()