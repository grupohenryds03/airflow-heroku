from airflow.models import DAG
from airflow.models import Variable
from airflow.decorators import task
from datetime import datetime
import snowflake.connector
import pandas as pd

conn = snowflake.connector.connect(
    user=Variable.get('snow_user'),
    password=Variable.get('snow_password'),
    account=Variable.get('snow_account'),
    warehouse=Variable.get('snow_warehouse'),
    database=Variable.get('snow_database')
    )
#funcion de coneccion a snowflake
def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

#conn = SnowflakeHook(snowflake_conn_id="snowflake_conn")




@task #tarea de extraccion de datos del WB y WHO
def extract_data() -> pd.DataFrame:
    import module.extract as ext
    return ext.etl_extract() #llama a la funcion de extracción (jhovany)

@task #tarea de limpieza de datos
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    import module.transform as tran
    
    return tran.etl_transform(df) #llama a la funcion de limpieza (rodri)

@task #tarea de guardar archivo csv comprimido a stage de snowflake
def load_data(df: pd.DataFrame):
    import tempfile
    
    temp_dir=tempfile.mkdtemp()
    
    df.to_csv(temp_dir +'/EV_limpio.csv', index=False)
    sql = f"PUT file://{temp_dir}/EV_limpio.csv @DATA_STAGE auto_compress=true"
    execute_query(conn, sql)
    #dwh_hook.get_first(sql)

#creación de las tareas con cronograma anual
with DAG(
   "ETL",
   start_date=datetime(year=2022, month=10, day=22),
   schedule_interval='@yearly',
   catchup=False
   ) as dag:
        df_crudo = extract_data() #sale data cruda
        df_limpio=transform_data(df_crudo) #entra data cruda, sale limpia
        load_data(df_limpio) # se guarda la data a snowflake