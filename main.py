from services.document import DocumentService
from services.labeler import Labeler
from services.summarizator import YaGPTSummary
from services.yadisk_scheduler import YaDiskScheduler
from backend.storage.ya_disk_storage import YaDiskStorage

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

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

FILES_NUM_SCHEDULER = int(Variable.get("FILES_NUM_SCHEDULER", default_var=50))  # add it to the cfg

    
document = DocumentService(document_service_config.endpoint)
summarizator = YaGPTSummary(ya_service_config.qa_token)
labeler = Labeler(labeler_service_config.hf_token, labeler_service_config.labels)
disk_scheduler = YaDiskScheduler(YaDiskStorage(YAServices.from_dict(y_data)), FILES_NUM_SCHEDULER)


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

scheduler >> news_service  # >> [summarization, labeler] >> "Отправка в тг"
