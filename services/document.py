import asyncio
from typing import List

from aiohttp import ClientSession


class DocumentService:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    async def __call__(self, session, text: str):
        async with session.post(self.endpoint, json={"content": text}) as post:
            if post.status:
                return (await post.json())['url']
            else:
                return ''

    async def push_news_to_service(self, news: List[str]):
        async with ClientSession() as session:
            tasks = []
            for new in news:
                task = asyncio.ensure_future(self(session, new))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses

    def run_push_news_to_service(self, **kwargs):
        ti = kwargs['ti']
        news = ti.xcom_pull(task_ids='get from storage')
        loop = asyncio.get_running_loop()
        future = asyncio.ensure_future(self.push_news_to_service(news))
        loop.run_until_complete(future)
        responses = future.result()
        return responses
