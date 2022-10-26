# Proyecto Final -Data 03- Soy Henry
## Jhovany Lara, Rodrigo Ruiz, Pablo Poletti ,José María Toledo

### Arquitectura: 
1. busqueda de data y análisis para data cruda
2. Ingesta data cruda, limpieza y carga (ETL)
3. Tareas para la carga incremental
4.  Ingesta de data a base de datos relacional
5. Acceso a base de datos para modelar progreciones en machine lerning y visualización en dasboard

- La arquitectura sigue cinco pasos principales: el primero para analizar las fuentes de datos, el segundo para la Extracción, Trasformación (limpieza) y Carga (Load) llamado por sus siglas ETL. El tercer paso donde se realiza la carga incremental a la base de datos relacional, el cuarto la carga incremental y el último paso donde se realizan las consultas necesarias para ser utilizada en modelos de ML y visualización en dashboard.
- El entorno de trabajo para el ETL se desarrola en AIRFLOW dentro de una cloud maching de HEROKU.
- Para el armado del datalake se ingestan los datos en el entorno STAGE de SNOWFLAKE en formato .csv comprimido en .gz (pueden ser tambien json, parquet, xlsx).
- En el caso de la base de datos relacional se utiliza SNOWFLAKE con la creación de un warehouse para su mantenimiento e ingesta incremental.
- para el modelado en ML y visualización de datos se realiza querys según los requerimientos del cliente.

<img src="/imagenes/diagrama solo.jpg"/>


- diagrama de arbol de repositorio en GITHUB para correr AIRFLOW en HEROKU.
```bash
airflow-heroku
├── dags
│   ├── ETL.py
│   └── module
│       ├── extract.py
│       └── transfom.py
├── .gitignore
├── airflow.cfg
├── app.json
├── Procfile
├── requirements.txt
├── runtime.txt
└── webserver_config.py
```