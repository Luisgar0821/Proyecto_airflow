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
def load_fact_table(json_data):
    df = pd.read_json(json_data)
    logging.info(f"Columnas recibidas en load_fact_table: {df.columns.tolist()}")

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO hechos_persona (
                edad, tasa_prevalencia, tasa_mortalidad, survival_years,
                id_pais, id_salud, id_exposicion, id_fumador,
                id_diagnostico, id_stage, id_tratamiento, id_historia
            ) VALUES (
                %s, %s, %s, %s,
                (SELECT id_pais FROM dim_pais WHERE country = %s LIMIT 1),
                (SELECT id_salud FROM dim_salud WHERE healthcare_access = %s AND early_detection = %s AND treatment_type = %s AND cancer_stage = %s LIMIT 1),
                (SELECT id_exposicion FROM dim_exposicion WHERE air_pollution_exposure = %s AND occupational_exposure = %s AND indoor_pollution = %s AND family_history = %s LIMIT 1),
                (SELECT id_fumador FROM dim_fumador WHERE fumador = %s AND fumador_pasivo = %s AND anios_fumando = %s AND cigarrillos_por_dia = %s LIMIT 1),
                (SELECT id_diagnostico FROM dim_diagnostico WHERE descripcion = %s LIMIT 1),
                (SELECT id_stage FROM dim_cancer_stage WHERE descripcion = %s LIMIT 1),
                (SELECT id_tratamiento FROM dim_tratamiento WHERE descripcion = %s LIMIT 1),
                (SELECT id_historia FROM dim_historia_familiar WHERE descripcion = %s LIMIT 1)
            )
        """, (
            row["age"], row["lung_cancer_prevalence_rate"], row["mortality_rate"], row["survival_years"],
            row["country"],
            row["healthcare_access"], row["early_detection"], row["treatment_type"], row["cancer_stage"],
            row["air_pollution_exposure"], row["occupational_exposure"], row["indoor_pollution"], row["family_history"],
            row["smoker"], row["passive_smoker"], row["years_of_smoking"], row["cigarettes_per_day"],
            row["lung_cancer_diagnosis"],
            row["cancer_stage"],
            row["treatment_type"],
            row["family_history"]
        ))

    conn.commit()
    cur.close()
    conn.close()
