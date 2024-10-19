from airflow import DAG
from datetime import datetime
import os
import sys

sys.path.insert(index:0, os.path.dirname(os.path.dirname(ox.path.abspath(__file__))))

default_args ={
    'owner': 'Oluwasegun',
    'start_date': datetime(2024, 10, 18)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='daily',
    catchup=False,
    max_active_runs=1
    tags=['reddit', 'etl','pipeline']
)



"""
#### Extraction from Reddit
"""
extract = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_postfix': f'refile_postfix
    },
    provide_context=True,
    dag=dag
)