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
    
    task_get_monthly_games = PythonOperator(
        task_id='get_monthly_games',
        python_callable=get_monthly_games,
        op_kwargs={
            'username':'nickgoldbergg'
        }
    )

    task_get_blitz_game_statistics = PythonOperator(
        task_id='get_blitz_game_statistics',
        python_callable=get_blitz_game_statistics,
        op_kwargs={
            'username':'nickgoldbergg'
        }
    )

    task_get_rapid_game_statistics = PythonOperator(
        task_id='get_blitz_game_statistics',
        python_callable=get_rapid_game_statistics,
        op_kwargs={
            'username':'nickgoldbergg'
        }
    )
    
    task_upload_monthly_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': 'resources/data/raw_monthly_games.json',
            'key':'test.csv',
            'bucket_name': 'chess-analytics-nickgoldbergg'
        }
    )

    task_upload_rapid_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': 'resources/data/raw_chess_rapid_statistics.json',
            'key':'test.csv',
            'bucket_name': 'chess-analytics-nickgoldbergg'
        }
    )

    task_upload_blitz_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': 'resources/data/raw_chess_blitz_statistics.json',
            'key':'test.csv',
            'bucket_name': 'chess-analytics-nickgoldbergg'
        }
    )

    task_get_monthly_games >> task_get_blitz_game_statistics >> task_get_rapid_game_statistics >> task_upload_monthly_to_s3 >> task_upload_rapid_to_s3 >> task_upload_blitz_to_s3

