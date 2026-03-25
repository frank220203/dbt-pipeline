from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from domains.ingest_usecase import IngestUsecase
from infrastructures.env.config import Config

config = Config()
svc_infos = config.get_svc_infos()
duck_path = config.get_duck_path()

with DAG(
    dag_id = "dbt-pipeline",
    catchup = config.catchup,
    schedule = "@once",
    start_date = datetime(2026, 3, 18)
) as dag:
    
    ingest_usecase = IngestUsecase()

    init = PythonOperator(
        task_id="init_mongo_to_duck",
        python_callable=ingest_usecase.create_dataset_table,
        op_kwargs={
            'svc_infos': svc_infos,
            'duck_path': duck_path
        }
    )

    mig_mongo_to_duck = PythonOperator(
        task_id="mig_mongo_to_duck",
        python_callable=ingest_usecase.mig_mongo_to_duck,
        op_kwargs={
            'svc_infos': svc_infos,
            'mongo_uri': config.get_mongo_uri(),
            'duck_path': duck_path
        }
    )

    unnest_portfolio = task_svc_a = BashOperator(
        task_id="unnest_portfolio",
        bash_command=f"dbt run --vars 'svc_nm: {svc_infos}'"
    )

    init >> mig_mongo_to_duck