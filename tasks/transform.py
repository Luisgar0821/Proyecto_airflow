from airflow.decorators import task
import pandas as pd
import logging

@task()
def transform_data(data_dict):
    """
    Recibe un diccionario de datos (output de extract), lo convierte en DataFrame,
    limpia los datos y retorna como JSON (serializable para Airflow).
    """
    df = pd.DataFrame.from_dict(data_dict)
    df.fillna("any", inplace=True)
    logging.info(f"Columnas después de limpieza: {df.columns.tolist()}")
    return df.to_json()