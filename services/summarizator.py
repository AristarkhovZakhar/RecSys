import asyncio
from typing import List
import re
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
            if not 'sharing_url' in js.keys():
                page = requests.get(url)
                soup = BeautifulSoup(page.text, "html.parser")
                info = {
                    'success': False,
                    'summary': soup.get_text().strip()
                }
                return {url: info}
            sharing_url = js['sharing_url']
            page = requests.get(sharing_url)
            soup = BeautifulSoup(page.text, "html.parser")
            summary = self.text_decode(soup.get_text()).replace('\n', '')
            summary = summary.replace('Пересказ YandexGPTYandexGPTкраткий пересказ статьи от нейросети', '')
            summary = re.sub('Для улучшения качествапредложите свой вариантСкопированоНе получилосьСкопировать.*', '', summary)
            info = {
                'success': True,
                'summary': '\n🔹'.join(summary.split('•'))
            }
        return {url: info}

    async def push_news_to_summarization(self, service_urls: List[str]):
        async with ClientSession() as session:
            tasks = []
            for new_url in service_urls:
                task = asyncio.ensure_future(self(session, new_url))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses

    def run_push_news_to_summarization(self, **kwargs):
        ti = kwargs['ti']
        service_urls = ti.xcom_pull(task_ids='run_push_news_to_service', key='document service')
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.push_news_to_summarization(service_urls))
        loop.run_until_complete(future)
        responses = future.result()
        to_push = {}
        print(responses)
        for i in responses:
            print(i)
            print(i.items())
            url, info = list(i.items())[0]
            to_push[url] = info
        ti.xcom_push(key='summarizator for ranker', value=to_push)
        return to_push
