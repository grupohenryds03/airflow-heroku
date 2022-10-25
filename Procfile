web: airflow db init
web: airflow create user -u admin -p admin -r Admin -f grupods03 -l henry -e grupods03@outlook.com
web: airflow webserver --port $PORT
web: airflow scheduler -D
worker: airflow celery worker
