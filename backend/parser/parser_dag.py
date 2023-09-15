from telegram_parser import TelegramParser
import json
import sys
from airflow import DAG
from airflow.operators.python import PythonOperator
from configs.config import API, Channels
from telethon import TelegramClient
from airflow.operators.empty import EmptyOperator
from datetime import datetime
sys.path.append("/home/parser/backend/")
from storage.ya_disk_storage import YaDiskStorage
from airflow.api.client.local_client import Client
with open("/home/parser/configs/api.json") as f:
    api = API.from_dict(json.load(f))
with open("/home/parser/configs/channels.json") as f:
    ap = json.load(f)
    channels = Channels.from_dict(ap)
client = TelegramClient(api.username, api.api_id, api.api_hash)
storage = YaDiskStorage("y0_AgAAAAAtTRFlAAnp9QAAAADjS1FmPbAPnfASRgapxZLElKH9_fQ_G3I")
parser = TelegramParser(client, channels.channels, storage)

parser_dag = DAG(
    dag_id='parser-dag',
    start_date=datetime(2023, 9, 15),
    schedule='@continuous',
    max_active_runs=1
)

python_task = PythonOperator(
    task_id='telegram-parser',
    python_callable=parser.run,
    dag=parser_dag
)

empty_task = EmptyOperator(
    task_id = 'end',
    dag = parser_dag,
)

python_task >> empty_task
