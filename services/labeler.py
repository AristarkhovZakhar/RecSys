from typing import List
import requests
import asyncio
from aiohttp import ClientSession
import nest_asyncio
from bs4 import BeautifulSoup
nest_asyncio.apply()

class Labeler:
    api_url = "https://api-inference.huggingface.co/models/MoritzLaurer/mDeBERTa-v3-base-xnli-multilingual-nli-2mil7"

    def __init__(self, hf_token: str, labels: List[str]):
        self.hf_token = hf_token
        self.headers = {"Authorization": f"Bearer {self.hf_token}"}
        self.labels = labels

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
        service_urls = ti.xcom_pull(task_ids='run_push_news_to_service')
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(self.push_news_to_labeler(service_urls))
        loop.run_until_complete(future)
        responses = future.result()
        return responses