from airflow.models import DAG
from airflow.operators.python import task
import snowflake.connector
from datetime import datetime
import module.transform as tran
import module.extract as ext
import pandas as pd
import tempfile


temp_dir=tempfile.mkdtemp()

conn = snowflake.connector.connect(
    user='grupods03',
    password='Henry2022#',
    account='nr28668.sa-east-1.aws',
    database='prueba',
    warehouse='dw_prueba',
    schema='public',
    insecure_mode=True)

def execute_query(connection, query):
    cursor = connection.cursor()
    cursor.execute(query)
    cursor.close()

@task
def extract_data() -> pd.DataFrame:
   return ext.extract()
@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
   return tran.transform(df)
@task
def load_data(df: pd.DataFrame):
    df.to_csv(temp_dir +'/EV_limpio.csv', index=False)
    sql = f"PUT file://{temp_dir}/EV_limpio.csv @DATA_STAGE auto_compress=true"
    execute_query(conn, sql)


with DAG(
   "ETL",
   default_args={'owner': 'airflow'},
   start_date=datetime(year=2022, month=10, day=22),
   schedule_interval='@yearly',
   catchup=False) as dag:
        df_crudo = extract_data()
        df_limpio=transform_data(df_crudo)
        load_data(df_limpio)