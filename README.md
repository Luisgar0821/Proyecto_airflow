# 🧬 ETL Lung Cancer Project with Apache Airflow

Este proyecto implementa un pipeline ETL completo utilizando **Apache Airflow** para procesar, transformar y cargar datos relacionados con el cáncer de pulmón en una base de datos **PostgreSQL**, empleando un modelo dimensional tipo **estrella**. El flujo permite análisis eficientes y estructurados a partir de un dataset enriquecido.

---

## 📌 Objetivo

Automatizar la extracción, transformación y carga de datos de cáncer de pulmón para su posterior análisis, integrando prácticas de ingeniería de datos y modelado dimensional.

---

## 🧱 Estructura del Proyecto

```
Proyecto_airflow/
│
├── dags/               # DAG principal de Airflow
│   └── etl_lung_cancer.py
├── tasks/              # Tareas ETL
│   ├── extract_task.py
│   ├── transform.py
│   ├── load_dimensions.py
│   └── load_fact.py
├── venv/               # Entorno virtual (incluido en Git)
├── requirements.txt    # Dependencias del proyecto
└── README.md
```

> Nota: aunque Airflow se usa en este proyecto, la carpeta `airflow/` está **excluida** en `.gitignore` y no forma parte del repositorio.

---

## ⚙️ Pipeline ETL en Airflow

El DAG `etl_lung_cancer` está compuesto por las siguientes tareas:

1. **Extracción (`extract_from_postgres`)**: carga los datos desde una tabla staging.
2. **Transformación (`transform_data`)**: limpieza y normalización del dataset.
3. **Carga de dimensiones (`load_dimensions`)**: inserta datos en las tablas dimensionales, evitando duplicados.
4. **Carga de hechos (`load_fact_table`)**: inserta registros con claves foráneas hacia las dimensiones.

---

## 📂 Modelo Dimensional

Modelo en estrella con la tabla de hechos `hechos_persona` al centro:

### Tabla de hechos

- `edad`
- `tasa_prevalencia`
- `tasa_mortalidad`
- `survival_years`
- `id_pais`
- `id_salud`
- `id_exposicion`
- `id_fumador`
- `id_diagnostico`
- `id_stage`
- `id_tratamiento`
- `id_historia`

### Tablas dimensionales

- `dim_pais(country, developed_or_developing, annual_deaths)`
- `dim_salud(healthcare_access, early_detection, treatment_type, cancer_stage)`
- `dim_exposicion(air_pollution_exposure, occupational_exposure, indoor_pollution, family_history)`
- `dim_fumador(smoker, passive_smoker, years_of_smoking, cigarettes_per_day)`
- `dim_diagnostico(descripcion)`
- `dim_cancer_stage(stage)`
- `dim_tratamiento(tratamiento)`
- `dim_historia_familiar(descripcion)`

---

## 🧪 Requisitos

### Instalación de dependencias

Crear y activar un entorno virtual:

```bash
python3 -m venv venv
source venv/bin/activate
```

Instalar los paquetes necesarios:

```bash
pip install -r requirements.txt
```

### Instalación de PostgreSQL

Instala PostgreSQL en sistemas Debian/Ubuntu:

```bash
sudo apt install postgresql postgresql-contrib
```

Establecer la contraseña del usuario `postgres`:

```bash
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'admin';"
```

### Instalación de Apache Airflow

Dentro del entorno virtual (`venv`) ejecutar:

```bash
pip install apache-airflow
```

---

## ▶️ Ejecución

1. Ejecutar el notebook de carga inicial de datos para poblar la tabla staging.
2. Exportar la ruta del proyecto como AIRFLOW_HOME:

```bash
export AIRFLOW_HOME=~/Proyecto_airflow/airflow
```

3. Inicializar el servidor web de Airflow:

```bash
airflow standalone
```

4. Activar el DAG `etl_lung_cancer` desde la interfaz web de Airflow.

---

## ❌ Consideraciones

- La base de datos debe estar en PostgreSQL local (`lung_cancer_database`).
- Se recomienda ejecutar el DAG en entorno controlado por `venv/`.
- La carpeta `airflow/` está ignorada por Git para evitar subir archivos innecesarios de ejecución.

---

## 🎬 Video demostrativo

_Agregue aquí el enlace a un video donde se muestra el funcionamiento del pipeline:_

```
[Ver video de demostración](enlace-al-video)
```

---

## 📌 Autor

**Luis Angel García**  
*Estudiante de IA*

**Yalany Willy Cardenas**  
*Estudiante de IA*

**Juliana Toro**  
*Estudiante de IA*

