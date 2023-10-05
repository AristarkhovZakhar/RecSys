import json
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from telethon import TelegramClient
import sys
sys.path.append("/opt/airflow/dags/")
sys.path.append("/opt/airflow/dags/backend/")
sys.path.append("/opt/airflow/dags/backend/parser/")
sys.path.append("/opt/airflow/dags/configs/")
from configs.config import APIConfig, ChannelsConfig
from telegram_parser import TelegramParser

with open("/opt/airflow/dags/configs/api.json") as f:
    api = APIConfig.from_dict(json.load(f))
with open("/opt/airflow/dags/configs/channels.json") as f:
    ap = json.load(f)
    channels = ChannelsConfig.from_dict(ap)

client = TelegramClient(api.username, api.api_id, api.api_hash)
parser = TelegramParser(client, channels)

parser_dag = DAG(
    dag_id='parser-dag',
    start_date=datetime(2023, 9, 28),
    max_active_runs=1
)

python_task = PythonOperator(
    task_id='telegram-parser',
    python_callable=parser.start_client,
    dag=parser_dag
)

empty_task = DummyOperator(
    task_id='end',
    dag=parser_dag,
)

python_task >> empty_task
