import asyncio
from typing import List

from aiohttp import ClientSession


class DocumentService:
    def __init__(self, endpoint: str):
        self.endpoint = endpoint

    async def __call__(self, session, text: str) -> str:
        async with session.post(self.endpoint, json={"content": text}) as post:
            if post.status:
                return (await post.json())['url']
            else:
                return ''

    async def push_news_to_service(self, news: List[str]) -> List[str]:
        async with ClientSession() as session:
            tasks = []
            for new in news:
                task = asyncio.ensure_future(self(session, new))
                tasks.append(task)
            responses = await asyncio.gather(*tasks)
        return responses

    def run_push_news_to_service(self, **kwargs) -> List[str]:
        ti = kwargs['ti']
        news = ti.xcom_pull(task_ids="run_push_from_disk", key="texts for webservice")
        print(news)
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self.push_news_to_service(news))
        loop.run_until_complete(future)
        responces = future.result()
        ti.xcom_push(key="document service", value=responces)
        return responces
