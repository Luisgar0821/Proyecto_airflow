from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
import os

# Configura tu conexión PostgreSQL
DB_PARAMS = {
    "host": "localhost",
    "database": "lung_cancer_database",
    "user": "postgres",
    "password": "admin"
}

def extract_from_postgres():
    conn = psycopg2.connect(**DB_PARAMS)
    df = pd.read_sql("SELECT * FROM lung_cancer_post", conn)
    df.to_csv("/tmp/lung_cancer_etl.csv", index=False)
    conn.close()


def transform_data():
    df = pd.read_csv("/tmp/lung_cancer_etl.csv")

    # Convertir nulos a 'any'
    df.fillna("any", inplace=True)

    # Guardar transformado
    df.to_csv("/tmp/lung_cancer_transformed.csv", index=False)


def load_dimensions():
    df = pd.read_csv("/tmp/lung_cancer_transformed.csv")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    # Cargar dim_pais con promedio de muertes por país
    df_pais = df.groupby("Country").agg({
        "Developed_or_Developing": "first",
        "Annual_Lung_Cancer_Deaths": "mean"
    }).reset_index()

    for _, row in df_pais.iterrows():
        cur.execute("""
            INSERT INTO dim_pais (country, developed_or_developing, annual_deaths)
            VALUES (%s, %s, %s)
        """, (row["Country"], row["Developed_or_Developing"], row["Annual_Lung_Cancer_Deaths"]))

    conn.commit()
    cur.close()
    conn.close()


def load_fact_table():
    df = pd.read_csv("/tmp/lung_cancer_transformed.csv")
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO hechos_persona (
                edad, fumador, fumador_pasivo, anios_fumando, cigarrillos_por_dia,
                tasa_prevalencia, tasa_mortalidad, id_pais, id_salud, id_exposicion
            ) VALUES (%s, %s, %s, %s, %s, %s, %s,
                (SELECT id_pais FROM dim_pais WHERE country = %s LIMIT 1),
                1, 1)
        """, (
            row["Age"], row["Smoker"], row["Passive_Smoker"], row["Years_of_Smoking"],
            row["Cigarettes_per_Day"], row["Lung_Cancer_Prevalence_Rate"], row["Mortality_Rate"],
            row["Country"]
        ))

    conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="etl_lung_cancer",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@once",
    catchup=False
) as dag:

    t1 = PythonOperator(task_id="extract", python_callable=extract_from_postgres)
    t2 = PythonOperator(task_id="transform", python_callable=transform_data)
    t3 = PythonOperator(task_id="load_dimensions", python_callable=load_dimensions)
    t4 = PythonOperator(task_id="load_fact", python_callable=load_fact_table)

    t1 >> t2 >> t3 >> t4
