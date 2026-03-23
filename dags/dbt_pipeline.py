from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from usecases.ingest_usecase import IngestUsecase

mongo_db = Variable.get("mongo_db", deserialize_json=True)

with DAG(
    dag_id = "dbt-pipeline",
    catchup = False,
    schedule = "@once",
    start_date = datetime(2026, 3, 18)
) as dag:
    ingest_usecase = IngestUsecase(Variable.get("mongo_uri"))

    ingest = PythonOperator(
        task_id="ingest_mongo_to_duck",
        python_callable=ingest_usecase.ingest_mongo_to_duck,
        op_kwargs={
            'db_name': mongo_db[0][0],
            'collections': mongo_db[1]
        }
    )