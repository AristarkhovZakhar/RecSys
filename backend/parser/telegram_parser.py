import json
import os
import sys
from queue import Queue
from typing import Tuple

from telethon import TelegramClient, events

sys.path.append("/Users/zakhar/Projects/RecSys/backend/")
sys.path.append("/Users/zakhar/Projects/RecSys/backend/storage")
from configs.config import API, Channels

from storage.ya_disk_storage import YaDiskStorage
from dataclasses import dataclass
from dataclasses_json import dataclass_json


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
    DATA_DIR = '/Users/zakhar/Projects/RecSys/backend/parser/data'

    def __init__(
            self,
            client: TelegramClient,
            channels: Channels,
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
        print(self.channels.channels.keys())
        @self.client.on(events.NewMessage)
        async def handle_message(event: events.NewMessage) -> None:
            os.makedirs(self.DATA_DIR, exist_ok=True)
            new = event.message
            if not new.out and str(event.chat_id) in self.channels.channels.keys() and new.message:
                print(new)
                print(event.chat_id)
                entity = await self.client.get_entity(event.chat_id)
                group_url = f"https://t.me/{entity.username}"
                text = new.message
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
                print(self.queue.qsize())
                if self.queue.qsize() >= self.n_auto_push_files:
                    filepathes = []
                    for _ in range(self.n_auto_push_files):
                        message = self.queue.get()
                        filepath = f"{self.DATA_DIR}/{self.filename_counter}.txt"
                        filepathes.append(filepath)
                        print(filepath)
                        with open(filepath, 'w') as f:
                            f.write(message.text)
                        self.filename_counter += 1
                    print(f"python3 push_to_storage.py --filepathes {' '.join(filepathes)}")
                    os.system(f"python3 push_to_storage.py --filepathes {' '.join(filepathes)}")

    def start_client(self) -> None:
        print('started')
        self.client.start()
        with self.client:
            self.client.run_until_disconnected()

    def preapre_message_to_upload(self, message: Message) -> Tuple[str, str]:
        filepath = f"{self.DATA_DIR}/{self.filename_counter}.txt"
        with open(filepath, 'w') as f:
            f.write(message.text)
        return filepath, str(self.filename_counter) + ".txt"


if __name__ == "__main__":
    with open("../../configs/api.json") as f:
        api = API.from_dict(json.load(f))
    with open("../../configs/channels.json") as f:
        ap = json.load(f)
        channels = Channels.from_dict(ap)
    client = TelegramClient(api.username, api.api_id, api.api_hash)
    parser = TelegramParser(client, channels)
    parser.start_client()