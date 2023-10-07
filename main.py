from services.document import DocumentService
from services.labeler import Labeler
from services.summarizator import YaGPTSummary
from services.yadisk_scheduler import YaDiskScheduler
from services.tg_poster import Ranging, TGPoster
from services.deleter import Deleter
from backend.storage.ya_disk_storage import YaDiskStorage
i

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

from datetime import datetime, timedelta

import json

from configs.config import DocumentServiceConfig, YAServiceConfig, LabelerConfig

import os
from queue import Queue
from typing import Tuple
from telethon import TelegramClient, events
import sys
#data_dir = "/home/parser"
data_dir = "/opt/airflow/dags"
sys.path.append(f"{data_dir}/")
sys.path.append(f"{data_dir}/backend/parser/")
sys.path.append(f"{data_dir}/configs/")
from configs.config import APIConfig, ChannelsConfig
from dataclasses import dataclass
from dataclasses_json import dataclass_json

with open(f"{data_dir}/configs/api.json") as f:
    api = APIConfig.from_dict(json.load(f))
with open(f"{data_dir}/configs/channels.json") as f:
    ap = json.load(f)
    channels = ChannelsConfig.from_dict(ap)
    client = TelegramClient(api.username, api.api_id, api.api_hash)

with open(f"{data_dir}/configs/document_service.json") as f:
    data = json.load(f)
    document_service_config = DocumentServiceConfig.from_dict(data)
with open(f"{data_dir}/configs/ya.json") as f:
    data = json.load(f)
    ya_service_config = YAServiceConfig.from_dict(data)
with open(f"{data_dir}/configs/labeler.json") as f:
    data = json.load(f)
    labeler_service_config = LabelerConfig.from_dict(data)

ROWS_TO_PUSH = int(Variable.get("ROWS_TO_PUSH_YADISK", default_var=5))

storage_dir = f"{data_dir}/backend/data/"
labeler_model_name = "MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7"

yadisk_scheduler = YaDiskScheduler(storage_dir, ROWS_TO_PUSH)
document = DocumentService(document_service_config.endpoint)
summarizator = YaGPTSummary(ya_service_config.qa_token)
labeler = Labeler(labeler_model_name, labeler_service_config.labels)
ranker_executor = Ranging()
deleter = Deleter()
poster_executor = TGPoster(client, channels)


with DAG(
        dag_id='main',
        start_date=datetime(2023, 10, 3),
        max_active_runs=2,
        max_active_tasks=2,
        dagrun_timeout=timedelta(hours=1),
        tags=["summarization", "labeling", "scoring", "ranking"],
        default_args={}
) as dag:
    scheduler = PythonOperator(
        task_id='run_push_from_disk',
        python_callable=yadisk_scheduler.run_push_service,
    )
    news_service = PythonOperator(
        task_id='run_push_news_to_service',
        python_callable=document.run_push_news_to_service,
    )
    summarization = PythonOperator(
        task_id='get_summary',
        python_callable=summarizator.run_push_news_to_summarization,
    )
    labeler = PythonOperator(
        task_id='get_labels',
        python_callable=labeler.run_push_news_to_labeler
    )
    ranker = PythonOperator(
        task_id='get_ranks',
        python_callable=ranker_executor.run_ranking_push_poster
    )
    poster = PythonOperator(
        task_id='post_news',
        python_callable=poster_executor.run_post_messages
    )
    deleter = PythonOperator(
        task_id='remove_files',
        python_callable=deleter.delete,
    )

scheduler >> news_service >> [summarization, labeler] >> ranker >> [poster, deleter]
