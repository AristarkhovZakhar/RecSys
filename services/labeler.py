import asyncio
import json
from typing import List, Dict, Any

import nest_asyncio
import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup
from transformers import pipeline
nest_asyncio.apply()
from airflow.models import Variable

class AsyncLabeler:
    api_url = "https://api-inference.huggingface.co/models/MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7"

    def __init__(self, hf_token: str, labels: List[str]):
        self.hf_token = hf_token
        self.headers = {"Authorization": f"Bearer {self.hf_token}"}
        self.labels = labels[:9]
        labels = Variable.get("LABELS", default_var=None)
        if labels is None:
            Variable.set("LABELS", self.labels)
            scores = {l: 1 for l in self.labels}
            counts = {l: 1 for l in self.labels}
            Variable.set('RANKER_SCORES', json.dumps(scores, ensure_ascii=False))
            Variable.set('RANKER_COUNTS', json.dumps(counts, ensure_ascii=False))

    async def __call__(self, session, url: str):
        page = requests.get(url)
        soup = BeautifulSoup(page.text, "html.parser")
        text = soup.find_all('p')[0].get_text()
        payload = {
            "inputs": text,
            "parameters": {"candidate_labels": self.labels}
        }
        async with session.post(self.api_url, headers=self.headers, json=payload) as post:
            js = await post.json()
            js["url"] = url
            return js

    async def push_news_to_labeler(self, service_urls: List[str]):
        async with ClientSession() as session:
            tasks = []
            for new_url in service_urls:
                task = asyncio.ensure_future(self(session, new_url))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses

    def run_push_news_to_labeler(self, **kwargs):
        ti = kwargs['ti']
        service_urls = ti.xcom_pull(task_ids='run_push_news_to_service', key="document service")
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.push_news_to_labeler(service_urls))
        loop.run_until_complete(future)
        responses = future.result()
        ti.xcom_push(key='labeler for ranker', value=responses)
        return responses


class Labeler:
    def __init__(self, model_name: str, labels: List[str]):
        self.classifier = pipeline("zero-shot-classification", model=model_name)
        self.labels = labels[:9]  # hardcode for this model
        self.labels += self.__get_unrecognized_label()
        labels = Variable.get("LABELS", default_var=None)
        if labels is None:
            Variable.set("LABELS", self.labels)
            scores = {l: 1 for l in self.labels}
            counts = {l: 1 for l in self.labels}
            Variable.set('RANKER_SCORES', json.dumps(scores, ensure_ascii=False))
            Variable.set('RANKER_COUNTS', json.dumps(counts, ensure_ascii=False))

    def __get_unrecognized_label(self) -> List[str]:
        result_str = ' not '.join(self.labels)
        return [result_str]

    def __call__(self, url: str) -> Dict[str, str]:
        page = requests.get(url)
        soup = BeautifulSoup(page.text, "html.parser")
        text = soup.find_all('p')[0].get_text()
        output = self.classifier(text, candidate_labels=self.labels, multi_label=False)
        output['text'] = text
        return {url: output}

    def run_push_news_to_labeler(self, **kwargs) -> Dict[str, str]:
        ti = kwargs['ti']
        service_urls = ti.xcom_pull(task_ids='run_push_news_to_service', key="document service")
        labeled_urls = []
        for url in service_urls:
            labeled_urls.append(self(url))
        to_push = {}
        for i in labeled_urls:
            url, output = list(i.items())[0]
            to_push[url] = output
        ti.xcom_push(key="labeler for ranker", value=to_push)
        return to_push
