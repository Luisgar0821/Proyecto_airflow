from airflow.decorators import task
import pandas as pd
import psycopg2
import logging

DB_PARAMS = {
    "host": "localhost",
    "database": "lung_cancer_database",
    "user": "postgres",
    "password": "admin"
}

@task()
def load_dimensions(json_data):
    df = pd.read_json(json_data)
    logging.info(f"Columnas recibidas en load_dimensions: {df.columns.tolist()}")
    logging.info("Primeras filas:\n%s", df.head())

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    # DIM PAIS
    df_pais = df.groupby("country").agg({
        "developed_or_developing": "first",
        "annual_lung_cancer_deaths": "mean"
    }).reset_index()

    for _, row in df_pais.iterrows():
        cur.execute("""
            INSERT INTO dim_pais (country, developed_or_developing, annual_deaths)
            VALUES (%s, %s, %s)
        """, (row["country"], row["developed_or_developing"], row["annual_lung_cancer_deaths"]))

    # DIM SALUD
    df_salud = df[["healthcare_access", "early_detection", "treatment_type", "cancer_stage"]].drop_duplicates()

    for _, row in df_salud.iterrows():
        cur.execute("""
            INSERT INTO dim_salud (healthcare_access, early_detection, treatment_type, cancer_stage)
            VALUES (%s, %s, %s, %s)
        """, (row["healthcare_access"], row["early_detection"], row["treatment_type"], row["cancer_stage"]))

    # DIM EXPOSICION
    df_expo = df[["air_pollution_exposure", "occupational_exposure", "indoor_pollution", "family_history"]].drop_duplicates()
    for _, row in df_expo.iterrows():
        cur.execute("""
            INSERT INTO dim_exposicion (air_pollution_exposure, occupational_exposure, indoor_pollution, family_history)
            VALUES (%s, %s, %s, %s)
        """, (row["air_pollution_exposure"], row["occupational_exposure"], row["indoor_pollution"], row["family_history"]))

    # DIM FUMADOR
    df_fumador = df[["smoker", "passive_smoker", "years_of_smoking", "cigarettes_per_day"]].drop_duplicates()
    for _, row in df_fumador.iterrows():
        cur.execute("""
            INSERT INTO dim_fumador (fumador, fumador_pasivo, anios_fumando, cigarrillos_por_dia)
            VALUES (%s, %s, %s, %s)
        """, (row["smoker"], row["passive_smoker"], row["years_of_smoking"], row["cigarettes_per_day"]))

    # OTRAS DIMENSIONES
    for col, table in [
        ("lung_cancer_diagnosis", "dim_diagnostico"),
        ("cancer_stage", "dim_cancer_stage"),
        ("treatment_type", "dim_tratamiento"),
        ("family_history", "dim_historia_familiar"),
    ]:
        valores = df[col].drop_duplicates()
        for val in valores:
            cur.execute(f"""
                INSERT INTO {table} (descripcion)
                VALUES (%s)
                ON CONFLICT (descripcion) DO NOTHING
            """, (val,))

    conn.commit()
    cur.close()
    conn.close()
