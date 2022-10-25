web: airflow db init
web: airflow create user -u admin -p admin -r Admin -f grupods03 -l henry -e grupods03@outlook.com
web: airflow webserver --port $PORT
worker: airflow celery worker & airflow scheduler