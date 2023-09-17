from services.document import DocumentService
from services.labeler import Labeler
from services.summarizator import YaGPTSummary
from services.yadisk_scheduler import YaDiskScheduler
from backend.storage.ya_disk_storage import YaDiskStorage

from datetime import datetime

import json

from dataclasses import dataclass
from dataclasses_json import dataclass_json

from configs.config import MainConfig
from configs.config import YAServices

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

with open('configs/main_config.json') as f:
    data = json.load(f)

with open('configs/ya.json') as f:
    y_data = json.load(f)

conf = MainConfig.from_json(data)
FILES_NUM_SCHEDULER = int(Variable.get("FILES_NUM_SCHEDULER", default_var=50))  # add it to the cfg

disk_scheduler = YaDiskScheduler(YaDiskStorage(YAServices.from_dict(y_data)), FILES_NUM_SCHEDULER)
document_service = DocumentService(conf.endpoint)
summarizator = YaGPTSummary(conf.yandex_qa_token)
labeler = Labeler(conf.hf_access_token)

with DAG(
        dag_id='main',
        start_date=datetime(2023, 9, 16),
        schedule_interval='@continuous',
) as dag:
    scheduler = PythonOperator(
        task_id='run_yadisk_scheduler',
        python_callable=disk_scheduler.run_push_service,
    )
    news_service = PythonOperator(
        task_id='run_push_news_to_service',
        python_callable=document_service.run_push_news_to_service,
    )
    summarization = PythonOperator(
        task_id='get_summary',
        python_callable=summarizator.run_push_news_to_summarization,
    )
    labeler = PythonOperator(
        task_id='get_labels',
        python_callable=labeler.run_push_news_to_labeler
    )

scheduler >> news_service  # >> [summarization, labeler] >> "Отправка в тг"
