from services.document import DocumentService
from services.labeler import Labeler
from services.summarizator import YaGPTSummary
from services.yadisk_scheduler import YaDiskScheduler
from backend.tg_poster import Ranging, TGPoster
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

ROWS_TO_PUSH = int(Variable.get("ROWS_TO_PUSH_YADISK", default_var=30))

yadisk_scheduler = YaDiskScheduler(YaDiskStorage(ya_service_config.qa_token), ROWS_TO_PUSH)
document = DocumentService(document_service_config.endpoint)
summarizator = YaGPTSummary(ya_service_config.qa_token)
labeler = Labeler(labeler_service_config.hf_token, labeler_service_config.labels)
ranker_executor = Ranging()
poster_executor = TGPoster() # Pass arguments


with DAG(
        dag_id='main',
        start_date=datetime(2023, 9, 16),
        schedule_interval='@continuous',
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


scheduler >> news_service >> [summarization, labeler] >> ranker >> poster
