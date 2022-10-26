# Proyecto Final -Data 03- Soy Henry
## Jhovany Lara, Rodrigo Ruiz, Pablo Poletti ,José María Toledo

### Arquitectura: Ingesta data cruda, limpieza y carga (ETL) -> Armado de tareas para la carga incremental -> Ingesta de data a base de datos relacional -> Acceso a base de datos para modelar progreciones en machine lerning y visualización en dasboard

- La arquitectura sigue tres pasos principales: uno para la Extracción, Trasformación (limpieza) y Carga (Load) llamado por sus siglas ETL, un segundo paso donde se realiza la carga incremental a la base de datos relacional y el trecero donde se realizan las consltas necesarias para ser utilizada en modelos de ML.
- El entorno de trabajo para el ETL se desarrola en AIRFLOW dentro de una cloud maching de HEROKU.
- Para el armado del datalake se ingestan los datos en el entorno STAGE de SNOWFLAKE en formato .csv comprimido en .gz (pueden ser tambien json, parquet, xlsx).
- En el caso de la base de datos relacional se utiliza SNOWFLAKE con la creación de un warehouse para su mantenimiento e ingesta incremental.
- para el modelado en ML y visualización de datos se realiza querys según los requerimientos del cliente.

<img src="/imagenes/diagrama solo.jpg"/>


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