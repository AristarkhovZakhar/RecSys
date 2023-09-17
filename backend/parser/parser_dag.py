import json
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from telethon import TelegramClient

from configs.config import APIConfig, ChannelsConfig
from telegram_parser import TelegramParser

with open("../configs/api.json") as f:
    api = APIConfig.from_dict(json.load(f))
with open("/home/parser/configs/channels.json") as f:
    ap = json.load(f)
    channels = ChannelsConfig.from_dict(ap)

client = TelegramClient(api.username, api.api_id, api.api_hash)
parser = TelegramParser(client, channels)

parser_dag = DAG(
    dag_id='parser-dag',
    start_date=datetime(2023, 9, 15),
    schedule='@continuous',
    max_active_runs=1
)

python_task = PythonOperator(
    task_id='telegram-parser',
    python_callable=parser.start_client,
    dag=parser_dag
)

empty_task = EmptyOperator(
    task_id='end',
    dag=parser_dag,
)

python_task >> empty_task
