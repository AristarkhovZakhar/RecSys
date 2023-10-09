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
            if not 'sharing_url' in js.keys():
                info = {
                    'success': False,
                    'summary': ''
                }
                return {url: info}
            sharing_url = js['sharing_url']
            page = requests.get(sharing_url)
            soup = BeautifulSoup(page.text, "html.parser")
            summary = self.text_decode(soup.get_text()).replace('\n', '').replace('Пересказ YandexGPTYandexGPTкраткий пересказ статьи от нейросети', '').replace(' Для улучшения качествапредложите свой вариантСкопированоНе получилосьСкопировать ссылкуПерейти на оригиналХорошийПлохойПожаловаться на пересказВойти© 2023 ООО «Яндекс»Пересказы основаны на оригинале,в них могут быть ошибки и неточностиПользовательское соглашениеAPIПожаловаться на пересказYandexGPTКак использовать APIПолучить токенУ\xa0сервиса есть REST-образный интерфейс, позволяющий автоматизироватьработу. Для того, чтобы им\xa0воспользоваться, достаточно получить токен ииспользовать его при походе в\xa0API.Пример:      >>> import requests>>> endpoint = \'https://300.ya.ru/api/sharing-url\'>>> response = requests.post(    endpoint,    json = {      \'article_url\': \'https://habr.com/ru/news/729422/\'    },    headers = {\'Authorization\': \'OAuth <token>\'})>>> response.json(){  "status": "success",  "sharing_url": "https://300.ya.ru/3fOcYRBL"}    ', '')
            info = {
                'success': True,
                'summary': summary
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
