from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from pathlib import Path

# Define o caminho base do projeto Meltano
MELTANO_PROJECT_DIR = Path(__file__).parents[2]


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'meltano_postgres_extract',
    default_args=default_args,
    description='DAG para executar pipeline ETL do Postgres usando Meltano',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['meltano', 'postgres', 'csv'],
) as dag:




    # Task para executar a extração do Postgres
    # extract_postgres = BashOperator(
    #     task_id='extract_postgres',
    #     bash_command=f'cd {MELTANO_PROJECT_DIR} && meltano run tap-postgres target-csv',
    # )

      # Tarefa para extrair dados do CSV
    extract_csv = BashOperator(
    task_id='extract_csv',
    bash_command=f'''
        cd {MELTANO_PROJECT_DIR} && \
        print(f"Meltano project directoryAAAAA: {MELTANO_PROJECT_DIR}") && \
        pwd && \
        print("Aquiiiiiii") && \
        ls -la && \
        meltano run tap-csv target-csv
    ''',
)


    # extract_postgres >> extract_csv
    extract_csv