import os
import sys
sys.path.append("../storage")
import json
import queue
import datetime, time

from typing import List, Dict, Any
from telethon.sync import TelegramClient

data_dir = "/opt/src"
sys.path.append(f"{data_dir}/")
sys.path.append(f"{data_dir}/configs")

from configs.config import APIConfig

class TGPoster:
    poster_data = f"{data_dir}/backend/poster_data"
    cache_duplicates = 100

    def __init__(
            self,
            client: TelegramClient,
            channels: List[str],
            post_quantity: int = 2
    ):
        print(channels, type(channels))
        self._client = client
        self.channels = channels
        self.post_quantity = post_quantity
        self.last_messages = {theme: set() for theme in list(self.channels.keys())}

    def _format_messages(self, item: Dict[str, str]):
        #message = f"**{'&'.join(item['labels'])}**:\n\n"
        message = f"{item['summary']}"
        return message

    def post_messages_all(self, messages: List[Dict[str, Any]]) -> None:
        for m in messages:
            for channel in self.channels:
                self._client.send_message(
                    channel,
                    self._format_messages(
                        {
                            'labels': m['labels'],
                            'summary': m['summary']
                        }
                    )
                )

    
    def post_messages(self, messages: List[Dict[str, Any]]) -> None:
        for m in messages:
            posted_channels = set()
            for theme in self.channels:
                hashed = m['summary'].__hash__()
                print(self.last_messages)
                
                try:
                    index = m['labels'].index(theme)
                    if 'Политика not Экономика not Социальная сфера not Международные отношения not Образование not Здравоохранение not Технологии not Культура not Наука и исследования' in m['labels']:
                        index = -1
                except:
                    index = -1

                if index == 0 and hashed not in self.last_messages[theme]:
                    for channel in self.channels[theme]:
                        if channel not in posted_channels:
                            print(channel, posted_channels)
                            self._client.send_message(
                                channel,
                                self._format_messages(
                                    {
                                        'labels': m['labels'],
                                        'summary': m['summary']
                                    }
                                )
                            )
                            self.last_messages[theme].add(hashed)
                            posted_channels.add(channel)
                            print(self.last_messages)

                            if len(self.last_messages[theme]) > self.cache_duplicates:
                                self.last_messages[theme] = {}

    def run_post_messages(self, **kwargs):
        ti = kwargs['ti']
        urls_news = ti.xcom_pull(task_ids='get_ranks', key='unformatted messages for posting')
        self.post_messages(urls_news)

    def listen(self):
        return sorted(os.listdir(self.poster_data), key=lambda x: int(x.split('.')[0]))

    def run_push_service(self, **kwargs):
        ti = kwargs['ti']
        ti.xcom_push(key='texts for webservice', value=texts)
        ti.xcom_push(key='filepathes to remove', value=filepathes)
        return texts
    
    def run(self):
        while True:
            if len(filepathes := self.listen()) > 3:
                filepath = filepathes[-1]
                print(filepathes[-1])
                path = os.path.join(self.poster_data, filepath)
                with open(path, "rb") as f:
                    try:
                        messages = json.load(f)
                        print(messages)
                    except json.JSONDecodeError as e:
                        print(f'With this file {f} error: \n\n {e}')
                
                if os.path.isfile(path):
                    os.system(f"rm -f {path}")

                self.post_messages(messages)
                minutesToSleep = 3 - datetime.datetime.now().minute % 3
                time.sleep(minutesToSleep * 60)

                print("-"*70)
    def start(self):
        self._client.loop.run_until_complete(self.run())
        self._client.disconnect()

if __name__ == "__main__":
    with open(f"{data_dir}/configs/api.json") as f:
        api = APIConfig.from_dict(json.load(f))
    with open(f"{data_dir}/configs/bot_token.json") as f:
        bot_token = json.load(f)['bot_token']
        bot = TelegramClient("bro3", api.api_id, api.api_hash).start(bot_token=bot_token)
    with open(f"{data_dir}/configs/post_channels_politics.json") as f:
        channels = json.load(f)["channels"]
        tg_p = TGPoster(bot, channels, 1)
    tg_p.run()
