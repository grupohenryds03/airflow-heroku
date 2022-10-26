# Proyecto Final -Data 03- Soy Henry
## Jhovany Lara, Rodrigo Ruiz, Pablo Poletti ,José María Toledo

### Arquitectura: 
1. busqueda de data y análisis para data cruda
2. Ingesta data cruda, limpieza y carga (ETL)
3. Tareas para la carga incremental
4.  Ingesta de data a base de datos relacional
5. Acceso a base de datos para modelar progreciones en machine lerning y visualización en dasboard

- La arquitectura sigue cinco pasos principales: el primero para analizar las fuentes de datos, el segundo para la Extracción, Trasformación (limpieza) y Carga (Load) llamado por sus siglas ETL. El tercer paso donde se realiza la carga incremental a la base de datos relacional, el cuarto la carga incremental y el último paso donde se realizan las consultas necesarias para ser utilizada en modelos de ML y visualización en dashboard.
- El entorno de trabajo para el ETL se desarrola en AIRFLOW dentro de una cloud maching de HEROKU. Acceso a la api: https://etl-latin-data.herokuapp.com/
- Para el armado del datalake se ingestan los datos en el entorno STAGE de SNOWFLAKE en formato .csv comprimido en .gz (pueden ser tambien json, parquet, xlsx).
- En el caso de la base de datos relacional se utiliza SNOWFLAKE con la creación de un warehouse para su mantenimiento e ingesta incremental.
- para el modelado en ML y visualización de datos se realiza querys según los requerimientos del cliente.

<img src="/imagenes/diagrama solo.jpg"/>


- diagrama de arbol de repositorio en GITHUB para correr AIRFLOW en HEROKU.

```bash
airflow-heroku
├── dags # carpeta de tareas a realizar
│   ├── ETL.py # modulo con cronograma de ETL anual
│   └── module
│       ├── extract.py # extracción cruda de data según selección(WHO/WB)
│       └── transfom.py # limpieza de datos
├── .gitignore # claves de acceso
├── airflow.cfg # setings de airflow
├── app.json # setingas de heroku
├── Procfile # setings para deploy de airfloy
├── requirements.txt # modulos de librererias a utlizar
├── runtime.txt # versión de phyton
└── webserver_config.py # configuración gral de la web 
```

- Tabla de datos en el STORAGE de SNOWFLAKE

| archivo                              | internal storage | tipo de compresión |
|--------------------------------------|------------------|--------------------|
| banco mundial.csv                    | snowflake        | .gz                |
| organización mundial de la salud.csv | snowflake        | .gz                |

- Para el armado del data warehouse se crean las tablas relacionales de hecho y dimensión con sus respectivos Id´s y primary keys.

1. tabla de hecho

| col     | tipo   | key | 
|---------|--------|-----|
| Idpais  | int    | PK  |
| Codpais | string | -   |
| año     | int    | -   |
| ValorVar| float  | -   |
| Idincome| int    | FK  |
| Idcont  | int    | FK  |
| IdVar   | int    | FK  |

2. tablas de dimesiones

income

| col    | tipo   | key |
|--------|--------|-----|
| idPais | int    | PK  |
| pais   | string | -   |
| income | string | -   |

geográfica

| col    | tipo   | key |
|--------|--------|-----|
| idPais | int    | PK  |
| pais   | string | -   |
| región | string | -   |

variables

| col    | tipo   | key |
|--------|--------|-----|
| idvar  | int    | PK  |
| codVar | string | -   |
| Desc   | string | -   |

