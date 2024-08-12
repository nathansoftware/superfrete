from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# Definir o DAG
default_args = {
    'owner': 'nathan pelicano',
    'depends_on_past': False,
    'start_date': datetime(2024, 8, 12), 
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'superfrete_futebol',
    default_args=default_args,
    description='DAG para executar o pipeline de dados de futebol',
    schedule_interval='0 15 * * *',  # Executa diariamente às 15:00
    catchup=False,
)

# Definir a tarefa para executar o script PySpark
execute_pyspark_script = BashOperator(
    task_id='execute_pyspark_script',
    bash_command='sf_extract_futebol.py',
    dag=dag,
)

execute_pyspark_script
