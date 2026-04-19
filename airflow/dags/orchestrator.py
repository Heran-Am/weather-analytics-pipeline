import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

sys.path.append('/opt/airflow/dags/api-request')
from insert_records import main

# def safe_main_callable():
#     from insert_records import main
#     return main()

default_args = {
    'description': 'A DAG to orchestrate weather data',
    'start_date': datetime(2025, 4, 30),
    'catchup': False,
}

dag = DAG(
    dag_id="weather_api_orchestrator",
    default_args=default_args,
    #description="A DAG to orchestrate weather data",
    schedule=timedelta(minutes=5),
)

with dag:
    task1 = PythonOperator(
        task_id="ingest_data_task",
        python_callable=main,
        
        task2 = DockerOperator(
        task_id="transform_data_task",
        image="ghcr.io/dbt-labs/dbt-postgres:1.9.latest",
        command="run",
        working_dir="/usr/app",
        mounts=[
            Mount(source="/home/heran/repos/weather-data-project/dbt/my_project",
                  target="/usr/app",
                  type= "bind"                  ),
            Mount(source="/home/heran/repos/weather-data-project/dbt/profiles/profiles.yml",
                  target="/root/.dbt/profiles.yml",
                  type= "bind"                  ),
            ],
        network_mode="my-network",
        docker_url="unix://var/run/docker.sock",
        auto_remove="sucess",
    )
    
    

    )
    task1 >> task2