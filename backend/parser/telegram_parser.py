import json
import os
import sys
from queue import Queue
from typing import Tuple

from telethon import TelegramClient, events
from telethon.tl.functions.channels import JoinChannelRequest

data_dir = "/home/parser"
# data_dir = "/opt/airflow/dags"
sys.path.append(f"{data_dir}/")
sys.path.append(f"{data_dir}/backend/parser/")
sys.path.append(f"{data_dir}/configs/")

from configs.config import APIConfig, ChannelsConfig
from dataclasses import dataclass
from dataclasses_json import dataclass_json
import logging

logging.basicConfig(level=logging.INFO, filename="parser_log.log", filemode="a")


@dataclass_json
@dataclass
class Media:
    photo: str
    webpage: str
    document: str


@dataclass_json
@dataclass
class Message:
    text: str
    url: str
    media: Media


class TelegramParser:
    DATA_DIR = f'{data_dir}/backend/parser/data'

    def __init__(
            self,
            client: TelegramClient,
            channels: ChannelsConfig,
            n_auto_push_files: int = 2

    ) -> None:
        super().__init__()
        self.client = client
        self.channels = channels
        self.queue = Queue()
        self.filename_counter = 0
        self.storage_process = None
        self.n_auto_push_files = n_auto_push_files

        os.makedirs(self.DATA_DIR, exist_ok=True)
        checked_media = ['photo', 'webpage', 'document']

        @self.client.on(events.NewMessage)
        async def handle_message(event: events.NewMessage) -> None:
            new = event.message
            if not new.out and str(event.chat_id) in self.channels.channels.keys() and new.message:
                entity = await self.client.get_entity(event.chat_id)
                group_url = f"https://t.me/{entity.username}"
                text = new.message
                print(text)
                message_url = os.path.join(group_url, str(new.id))
                collected_info = {}
                if hasattr(new.media, 'photo'):
                    collected_info['photo'] = str(new.media.photo.id)
                elif hasattr(new.media, 'webpage'):
                    collected_info['webpage'] = str(new.media.webpage.id)
                elif hasattr(new.media, 'document'):
                    collected_info['document'] = str(new.media.document.id)
                for m in checked_media:
                    if not m in collected_info.keys():
                        collected_info[m] = ''
                media = Media.from_dict(collected_info)
                message = Message(text, message_url, media)
                self.queue.put(message)
                logging.info(self.queue.qsize())
                if self.queue.qsize() >= self.n_auto_push_files:
                    filepathes = []
                    for _ in range(self.n_auto_push_files):
                        message = self.queue.get()
                        logging.info(message)
                        filepath = f"{self.DATA_DIR}/{self.filename_counter}.txt"
                        filepathes.append(filepath)
                        with open(filepath, 'w') as f:
                            f.write(message.text)
                            logging.info(f"filename counter: {self.filename_counter}")
                        self.filename_counter += 1
                    logging.info(f"python3 push_to_storage.py --filepathes {' '.join(filepathes)}")
                    print(f"python3 push_to_storage.py --filepathes {' '.join(filepathes)}")
                    os.system(f"python3 push_to_storage.py --filepathes {' '.join(filepathes)}")

    def start_client(self) -> None:
        logging.info("START PARSER")
        print("start parser")
        self.client.start()
        with self.client:
            self.client.run_until_disconnected()

    def preapre_message_to_upload(self, message: Message) -> Tuple[str, str]:
        filepath = f"{self.DATA_DIR}/{self.filename_counter}.txt"
        with open(filepath, 'w') as f:
            f.write(message.text)
        return filepath, f"{self.filename_counter}.txt"


async def main(parser: TelegramParser):
    for link in parser.channels.channels.values():
        try:
            channel = await client.get_entity(link)
            await client(JoinChannelRequest(channel))
            print(channel)
        except:
            continue

    dialogs = await parser.client.get_dialogs()
    channels = [dialog for dialog in dialogs if dialog.is_channel]
    for channel in channels:
        print(f"Название: {channel.title}, Идентификатор: {channel.id}")


if __name__ == "__main__":
    with open(f"{data_dir}/configs/api.json") as f:
        api = APIConfig.from_dict(json.load(f))
    with open(f"{data_dir}/configs/channels.json") as f:
        ap = json.load(f)
        channels = ChannelsConfig.from_dict(ap)
    client = TelegramClient(api.username, api.api_id, api.api_hash)
    parser = TelegramParser(client, channels)
    with client:
        client.loop.run_until_complete(main(parser))
    parser.start_client()
