import psycopg2
import pandas as pd

def crear_usuario_y_bd(admin_user="postgres", admin_pass="admin"):
    conn = psycopg2.connect(
        dbname="postgres",
        user=admin_user,
        password=admin_pass,
        host="localhost"
    )
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("SELECT 1 FROM pg_roles WHERE rolname='etl_user'")
    if not cur.fetchone():
        cur.execute("CREATE USER etl_user WITH PASSWORD 'etl_pass'")
        print("Usuario 'etl_user' creado.")
    else:
        print("El usuario 'etl_user' ya existe.")

    cur.execute("SELECT 1 FROM pg_database WHERE datname='lung_cancer_database'")
    if not cur.fetchone():
        cur.execute("CREATE DATABASE lung_cancer_database OWNER etl_user")
        print("Base de datos 'lung_cancer_database' creada.")
    else:
        print("La base de datos 'lung_cancer_database' ya existe.")

    cur.close()
    conn.close()

def conectar_etl():
    return psycopg2.connect(
        dbname="lung_cancer_database",
        user="etl_user",
        password="etl_pass",
        host="localhost"
    )

def crear_tabla_lung_cancer_pre(conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lung_cancer_pre (
            id SERIAL PRIMARY KEY,
            Country VARCHAR(255),
            Population_Size INT,
            Age INT,
            Gender VARCHAR(20),
            Smoker VARCHAR(20),
            Years_of_Smoking INT,
            Cigarettes_per_Day INT,
            Passive_Smoker VARCHAR(20),
            Family_History VARCHAR(20),
            Lung_Cancer_Diagnosis VARCHAR(20),
            Cancer_Stage VARCHAR(20),
            Survival_Years INT,
            Adenocarcinoma_Type VARCHAR(20),
            Air_Pollution_Exposure VARCHAR(20),
            Occupational_Exposure VARCHAR(20),
            Indoor_Pollution VARCHAR(20),
            Healthcare_Access VARCHAR(20),
            Early_Detection VARCHAR(20),
            Treatment_Type VARCHAR(20),
            Developed_or_Developing VARCHAR(20),
            Annual_Lung_Cancer_Deaths INT,
            Lung_Cancer_Prevalence_Rate FLOAT,
            Mortality_Rate FLOAT);
    """)
    conn.commit()
    cur.close()
    print("Tabla 'lung_cancer_pre' verificada/creada.")

def insertar_datos_lung_cancer_pre(conn, df):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lung_cancer_pre")
    if cur.fetchone()[0] == 0:
        for _, row in df.iterrows():
            cur.execute("""
                INSERT INTO lung_cancer_dirty (Country, Population_Size, Age,Gender, Smoker, Years_of_Smoking, Cigarettes_per_Day, Passive_Smoker, Family_History, Lung_Cancer_Diagnosis, Cancer_Stage, Survival_Years, Adenocarcinoma_Type, Air_Pollution_Exposure, Occupational_Exposure, Indoor_Pollution, Healthcare_Access, Early_Detection, Treatment_Type, Developed_or_Developing, Annual_Lung_Cancer_Deaths, Lung_Cancer_Prevalence_Rate, Mortality_Rate)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (row['Country'],row['Population_Size'],row['Age'],row['Gender'],row['Smoker'],row['Years_of_Smoking'],row['Cigarettes_per_Day'],row['Passive_Smoker'],row['Family_History'],row['Lung_Cancer_Diagnosis'],row['Cancer_Stage'],row['Survival_Years'],row['Adenocarcinoma_Type'],row['Air_Pollution_Exposure'],row['Occupational_Exposure'],row['Indoor_Pollution'],row['Healthcare_Access'],row['Early_Detection'],row['Treatment_Type'],row['Developed_or_Developing'],row['Annual_Lung_Cancer_Deaths'],row['Lung_Cancer_Prevalence_Rate'],row['Mortality_Rate'])
            )
        conn.commit()
        print("Datos insertados en 'lung_cancer_pre'")
    else:
        print("Los datos ya están insertados. No se duplicaron registros.")
    cur.close()

def crear_tabla_lung_cancer_post (conn):
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS lung_cancer_post (
            id SERIAL PRIMARY KEY,
            Age INT,
            Country VARCHAR(20),
            Lung_Cancer_Prevalence_Rate FLOAT,
            Smoker BOOLEAN,
            Years_of_Smoking INT,
            Cigarettes_per_Day INT,
            Passive_Smoker BOOLEAN,
            Lung_Cancer_Diagnosis BOOLEAN,
            Healthcare_Access VARCHAR(20),
            Early_Detection BOOLEAN,
            Survival_Years INT,
            Developed_or_Developing VARCHAR(20),
            Mortality_Rate FLOAT,
            Annual_Lung_Cancer_Deaths INT,
            Air_Pollution_Exposure VARCHAR(20),
            Occupational_Exposure BOOLEAN,
            Indoor_Pollution BOOLEAN,
            Family_History BOOLEAN,
            Treatment_Type VARCHAR(20),
            Cancer_Stage VARCHAR(20));
    """)
    conn.commit()
    cur.close()
    print("Tabla 'lung_cancer_post' verificada/creada.")

def insertar_datos_lung_cancer_post(conn, df_selected):
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM lung_cancer_post")
    if cur.fetchone()[0] == 0:
        for _, row in df_selected.iterrows():
            cur.execute("""
                   INSERT INTO lung_cancer_post (Age,Country,Lung_Cancer_Prevalence_Rate,Smoker,Years_of_Smoking,Cigarettes_per_Day,Passive_Smoker,Lung_Cancer_Diagnosis,Healthcare_Access,Early_Detection,Survival_Years,Developed_or_Developing,Mortality_Rate,Annual_Lung_Cancer_Deaths,Air_Pollution_Exposure,Occupational_Exposure,Indoor_Pollution,Family_History,Treatment_Type,Cancer_Stage)
                   VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (row['Age'],row['Country'],row['Lung_Cancer_Prevalence_Rate'],row['Smoker'],row['Years_of_Smoking'],row['Cigarettes_per_Day'],row['Passive_Smoker'],row['Lung_Cancer_Diagnosis'],row['Healthcare_Access'],row['Early_Detection'],row['Survival_Years'],row['Developed_or_Developing'],row['Mortality_Rate'],row['Annual_Lung_Cancer_Deaths'],row['Air_Pollution_Exposure'],row['Occupational_Exposure'],row['Indoor_Pollution'],row['Family_History'],row['Treatment_Type'],row['Cancer_Stage'])
            )
        conn.commit()
        print("Datos insertados en 'lung_cancer_post'")
    else:
        print("Los datos ya están insertados. No se duplicaron registros.")
    cur.close()

def leer_datos_lung_cancer_post(conn):
    """
    Lee todos los datos de la tabla lung_cancer_post y los retorna como un DataFrame.
    """
    cur = conn.cursor()
    try:
        cur.execute("SELECT COUNT(*) FROM lung_cancer_post")
        count = cur.fetchone()[0]

        if count == 0:
            print("La tabla 'lung_cancer_post' está vacía.")
            return pd.DataFrame()  # Retorna un DataFrame vacío
        else:
            df = pd.read_sql("SELECT * FROM lung_cancer_post", conn)
            print(f"Se leyeron {len(df)} registros de 'lung_cancer_post'")
            return df

    except Exception as e:
        print(f"Error al leer la tabla 'lung_cancer_post': {e}")
        return pd.DataFrame()
    finally:
        cur.close()