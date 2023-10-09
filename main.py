import json
import sys
from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from telethon import TelegramClient

from configs.config import DocumentServiceConfig, YAServiceConfig, LabelerConfig
from services.deleter import Deleter
from services.document import DocumentService
from services.labeler import Labeler
from services.summarizator import YaGPTSummary
from services.ranker import Ranker
from services.tg_poster import TGPoster
from services.yadisk_scheduler import YaDiskScheduler

# data_dir = "/home/parser"
data_dir = "/opt/airflow/dags"
sys.path.append(f"{data_dir}/")
sys.path.append(f"{data_dir}/backend/parser/")
sys.path.append(f"{data_dir}/configs/")
from configs.config import APIConfig, ChannelsConfig

with open(f"{data_dir}/configs/api.json") as f:
    api = APIConfig.from_dict(json.load(f))
with open(f"{data_dir}/configs/channels.json") as f:
    ap = json.load(f)
    channels = ChannelsConfig.from_dict(ap)

with open(f"{data_dir}/configs/document_service.json") as f:
    data = json.load(f)
    document_service_config = DocumentServiceConfig.from_dict(data)
with open(f"{data_dir}/configs/ya.json") as f:
    data = json.load(f)
    ya_service_config = YAServiceConfig.from_dict(data)
with open(f"{data_dir}/configs/labeler.json") as f:
    data = json.load(f)
    labeler_service_config = LabelerConfig.from_dict(data)
with open(f"{data_dir}/configs/bot_token.json") as f:
    bot_token = json.load(f)['bot_token']
with open(f"{data_dir}/configs/bot_token.json") as f:
    bot_token = json.load(f)['bot_token']
    bot = TelegramClient("bro", api.api_id, api.api_hash).start(bot_token=bot_token)


storage_dir = f"{data_dir}/backend/data/"
labeler_model_name = "MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7"
sbert_model_name = "all-MiniLM-L6-v2"

document = DocumentService(document_service_config.endpoint)
summarizator = YaGPTSummary(ya_service_config.qa_token)
labeler = Labeler(labeler_model_name, labeler_service_config.labels)
ranker = Ranker(model_name=sbert_model_name)
deleter = Deleter()
poster = TGPoster(bot, channels)


#scores = {l: 1 for l in labeler_service_config.labels}
#counts = {l: 1 for l in labeler_service_config.labels}
#Variable.set('RANKER_SCORES', scores)
#Variable.set('RANKER_COUNTS', counts)

ROWS_TO_PUSH = int(Variable.get("ROWS_TO_PUSH_YADISK", default_var=2))
yadisk_scheduler = YaDiskScheduler(storage_dir, ROWS_TO_PUSH)

with DAG(
        dag_id='main',
        start_date=datetime(2023, 10, 3)
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
        python_callable=ranker.run_ranking_push_poster
    )
    poster = PythonOperator(
        task_id='post_news',
        python_callable=poster.run_post_messages
    )
    deleter = PythonOperator(
        task_id='remove_files',
        python_callable=deleter.delete,
    )

scheduler >> news_service >> summarization >> labeler >> ranker >> [poster,
deleter]
