from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator

import asyncio
import json
from chessdotcom.aio import get_player_stats, get_player_games_by_month, Client
from datetime import datetime

Client.aio = True

async def gather_cors(cor):
    response = await asyncio.gather(cor)
    return response

currentMonth = datetime.now().month
currentYear = datetime.now().year
currentDay = datetime.now().day
date_string = f"{currentYear}-{currentMonth}-{currentDay}"

def get_monthly_games(username, year=currentYear, month=currentMonth):
    cor = get_player_games_by_month(username=username,year=year, month=month)
    response = asyncio.run(cor).json
    raw_monthly_games = response['games']

    return raw_monthly_games

def get_rapid_game_statistics(username):
    cor = get_player_stats(username=username)
    response = asyncio.run(cor).json
    raw_chess_rapid_statistics = response['stats']['chess_rapid']

    return raw_chess_rapid_statistics

def get_blitz_game_statistics(username):
    cor = get_player_stats(username=username)
    response = asyncio.run(cor).json
    raw_chess_blitz_statistics = response['stats']['chess_blitz']

    return raw_chess_blitz_statistics

with DAG('s3_upload_xcom', 
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

    task_get_rapid_game_statistics = PythonOperator(
        task_id='get_rapid_game_statistics',
        python_callable=get_rapid_game_statistics,
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

    task_upload_monthly_to_s3 = S3CreateObjectOperator(
        task_id='upload_to_s3_monthly',
        aws_conn_id='s3_conn',
        s3_bucket='chess-analytics-nickgoldbergg',
        s3_key=f'raw/{date_string}/raw_monthly_games.json',
        data='{{ task_instance.xcom_pull(task_ids="task_get_monthly_games") }}'
    )

    task_upload_rapid_to_s3 = S3CreateObjectOperator(
        task_id='upload_to_s3_rapid',
        aws_conn_id='s3_conn',
        s3_bucket='chess-analytics-nickgoldbergg',
        s3_key=f'raw/{date_string}/raw_chess_rapid_statistics.json',
        data='{{ task_instance.xcom_pull(task_ids="task_get_rapid_game_statistics") }}'
    )

    task_upload_blitz_to_s3 = S3CreateObjectOperator(
        task_id='upload_to_s3_blitz',
        aws_conn_id='s3_conn',
        s3_bucket='chess-analytics-nickgoldbergg',
        s3_key=f'raw/{date_string}/raw_chess_blitz_statistics.json',
        data='{{ task_instance.xcom_pull(task_ids="task_get_blitz_game_statistics") }}'
    )

    task_get_monthly_games >> task_get_rapid_game_statistics >> task_get_blitz_game_statistics >> task_upload_monthly_to_s3 >> task_upload_rapid_to_s3 >> task_upload_blitz_to_s3