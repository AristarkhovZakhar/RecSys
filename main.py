from services.document import DocumentService
from services.labeler import Labeler
from services.summarizator import YaGPTSummary

from datetime import datetime

import json

from dataclasses import dataclass
from dataclasses_json import dataclass_json

from configs.config import MainConfig

from airflow import DAG
from airflow.operators.python import PythonOperator

with open('configs/main_config.json') as f:
    data = json.load(f)

conf = MainConfig.from_json(data)

document_service = DocumentService(conf.endpoint)
summarizator = YaGPTSummary(conf.yandex_qa_token)
labeler = Labeler(conf.hf_access_token)

with DAG(
    dag_id='main',
    start_date=datetime(2023, 9, 16),
    schedule_interval='@continuous',
) as dag:

    news_service = PythonOperator(
            task_id='run_push_news_to_service',
            python_callable=document_service.run_push_news_to_service,
        )
    summarization = PythonOperator(
            task_id='get_summary',
            python_callable=summarizator.run_push_news_to_summarization,
        )
    labeler = PythonOperator(
        task_id = 'get_labels',
        python_callable=labeler.run_push_news_to_labeler
        )


"Шедулер по я диску" >> news_service >> [summarization, labeler] >> "Отправка в тг"