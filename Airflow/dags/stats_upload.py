from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

import asyncio
import json
from chessdotcom.aio import get_player_stats, Client
from datetime import datetime

Client.aio = True

async def gather_cors(cor):
    response = await asyncio.gather(cor)
    return response

currentMonth = datetime.now().month
currentYear = datetime.now().year
currentDay = datetime.now().day
date_string = f"{currentYear}-{currentMonth}-{currentDay}"

def get_game_statistics(username):
    cor = get_player_stats(username=username)
    response = asyncio.run(cor).json
    game_stats = response['stats']

    return game_stats

with DAG('stats_upload', 
         start_date=datetime(2022, 1, 1),
         schedule_interval='@daily', 
         catchup=False) as dag:

    task_get_game_statistics = PythonOperator(
        task_id='get_game_statistics',
        python_callable=get_game_statistics,
        op_kwargs={
            'username':'nickgoldbergg'
        }
    )

    task_upload_stats_to_s3 = S3CreateObjectOperator(
        task_id='upload_stats_to_s3',
        aws_conn_id='s3_conn',
        s3_bucket='chess-analytics-nickgoldbergg',
        s3_key=f'raw/{date_string}/chess_statistics.json',
        data='{{ task_instance.xcom_pull(task_ids="task_get_game_statistics") }}'
    )

    task_get_game_statistics >> task_upload_stats_to_s3