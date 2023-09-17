import asyncio
from typing import List

import requests
from aiohttp import ClientSession
from bs4 import BeautifulSoup


class YaGPTSummary:
    '''
    class for get summary by text
    https://300.ya.ru
    '''

    def __init__(self, token: str):
        self.__token = token

    @staticmethod
    def text_decode(text: str):
        bstring = bytes(ord(c) for c in text)
        result = bstring.decode('utf-8')
        return result

    async def __call__(self, session, url: str):
        endpoint = 'https://300.ya.ru/api/sharing-url'
        async with session.post(endpoint, json={'article_url': url},
                                headers={'Authorization': f'OAuth {self.__token}'}) as post:
            js = await post.json()
            url = js['sharing_url']
            page = requests.get(url)
            soup = BeautifulSoup(page.text, "html.parser")
            title = self.text_decode(soup.find_all('h1', class_='title svelte-jpt3zn')[0].get_text()).strip()
            tokens = [self.text_decode(item.get_text()).replace('• ', '').strip() for item in
                      soup.find_all('li', class_='theses-item svelte-1tflzpo')]
            summary = "".join(tokens)
            info = {
                'title': title,
                'tokens': tokens,
                'summary': summary
            }
        return info

    async def push_news_to_summarization(self, service_urls: List[str]):
        async with ClientSession() as session:
            tasks = []
            for new_url in service_urls:
                task = asyncio.ensure_future(self(session, new_url))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses

    def run_push_news_to_service(self, **kwargs):
        ti = kwargs['ti']
        service_urls = ti.xcom_pull(task_ids='run_push_news_to_service')
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(self.push_news_to_summarization(service_urls))
        loop.run_until_complete(future)
        responses = future.result()
        return responses
