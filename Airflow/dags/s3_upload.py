from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

from extract_functions import get_monthly_games, get_blitz_game_statistics, get_rapid_game_statistics
from datetime import datetime

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG('s3_upload', 
         start_date=datetime(2022, 1, 1),
         schedule_interval='@monthly', 
         catchup=False) as dag:
    
    
    
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/Users/nicholas.goldberg/test-project/test.csv',
            'key':'test.csv',
            'bucket_name': 'chess-analytics-nickgoldbergg'
        }
    )

