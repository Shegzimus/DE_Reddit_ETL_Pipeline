from airflow import DAG
from datetime import datetime
import os
import sys
from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.reddit_pipeline import reddit_pipeline

default_args ={
    'owner': 'Oluwasegun',
    'start_date': datetime(2024, 10, 18)
}

file_postfix = datetime.now().strftime("%Y%m%d")

dag = DAG(
    dag_id='etl_reddit_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    max_active_runs=1,
    tags=['reddit', 'etl','pipeline']
)



"""
#### Extraction from Reddit
"""
extract = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=reddit_pipeline,
    op_kwargs={
        'file_name': f'reddit_{file_postfix}',
        'subreddit':'dataengineering',
        'time_filter': 'day',
        'limit': 200
    },
    provide_context=True,
    dag=dag
)