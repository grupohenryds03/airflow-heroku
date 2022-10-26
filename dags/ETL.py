from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
import snowflake.connector
import tempfile
import module.transform as tran
import module.extract as ext


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


def extract_file(temp_dir) -> str:
    return ext.etl_extract()

def file_transform(temp_dir,ti) -> str:
    return tran.etl_transform(ti)

def file_to_stage(ti) -> None:
    sql=ti.xcom_pull(task_ids='transform')
    execute_query(conn, sql)

with DAG(
    dag_id='ETL',
    schedule_interval='@yearly',
    start_date=datetime(year=2022, month=10, day=22),
    catchup=False) as dag:

    extract=PythonOperator(
        task_id='extract',
        python_callable=extract_file,
    )
    transform=PythonOperator(
        task_id='transform',
        python_callable=file_transform,
        do_xcom_push=True
    )
    load=PythonOperator(
        task_id='load',
        python_callable=file_to_stage
    )

    extract >> transform >> load
