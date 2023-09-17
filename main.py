from services.document import DocumentService
from services.labeler import Labeler
from services.summarizator import YaGPTSummary

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime

import json

from configs.config import DocumentServiceConfig, YAServiceConfig, LabelerConfig

with open("configs/document_service.json") as f:
    data = json.load(f)
    document_service_config = DocumentServiceConfig.from_dict(data)
with open("configs/ya.json") as f:
    data = json.load(f)
    ya_service_config = YAServiceConfig.from_dict(data)
with open("configs/labeler.json") as f:
    data = json.load(f)
    labeler_service_config = LabelerConfig.from_dict(data)

document = DocumentService(document_service_config.endpoint)
summarizator = YaGPTSummary(ya_service_config.qa_token)
labeler = Labeler(labeler_service_config.hf_token, labeler_service_config.labels)

with DAG(
    dag_id='main',
    start_date=datetime(2023, 9, 16),
    schedule_interval='@continuous',
) as dag:

    news_service = PythonOperator(
            task_id='run_push_news_to_service',
            python_callable=document.run_push_news_to_service,
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
