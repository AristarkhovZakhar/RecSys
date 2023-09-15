import multiprocessing as mp
import sys
import os
import json
from telethon import TelegramClient, events
from typing import Dict, Any, Tuple
from multiprocessing import Process, Queue
sys.path.append("/home/parser/")
from configs.config import API, Channels
from parser_abstract import ParserInterface
sys.path.append("/home/parser/backend/")
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


class TelegramParser(ParserInterface):
    DATA_DIR = './data'

    def __init__(
            self,
            client: TelegramClient,
            channels: Channels,
            storage: YaDiskStorage
    ) -> None:
        super().__init__()
        self.client = client
        self.channels = channels
        self.storage = storage
        self.queue = Queue()
        self.filename_counter = 0
        self.storage_process = None

        os.makedirs(self.DATA_DIR, exist_ok=True)
        checked_media = ['photo', 'webpage', 'document']

        @client.on(events.NewMessage)
        async def handle_message(event: events.NewMessage) -> None:
            new = event.message
            print(new)
            if not new.out and event.chat_id in self.channels.channels.keys():
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

    def start_client(self) -> None:
        self.client.start()
        with self.client:
            self.client.run_until_disconnected()

    def preapre_message_to_upload(self, message: Message) -> Tuple[str, str]:
        filepath = f"{self.DATA_DIR}/{self.filename_counter}.txt"
        with open(filepath, 'w') as f:
            f.write(message.text)
        return filepath, str(self.filename_counter) + ".txt"

    def storage_messages(self):
        while True:
            if not self.queue.empty():
                message = self.queue.get()
                try:
                    self.storage.upload(*self.preapre_message_to_upload(message))
                    self.filename_counter += 1
                except Exception:
                    continue

    def run(self):
        self.storage_process = Process(target=self.storage_messages())
        self.storage_process.start()
        self.start_client()

    def stop(self):
        self.storage_process.terminate()
        self.storage_process.join()
        if self.client.is_connected():
            self.client.disconnect()


if __name__ == "__main__":
    mp.set_start_method('fork')
    with open("../../configs/api.json") as f:
        api = API.from_dict(json.load(f))
    with open("../../configs/channels.json") as f:
        ap = json.load(f)
        channels = Channels.from_dict(ap)
    client = TelegramClient(api.username, api.api_id, api.api_hash)
    storage = YaDiskStorage("y0_AgAAAAAtTRFlAAnp9QAAAADjS1FmPbAPnfASRgapxZLElKH9_fQ_G3I")
    parser = TelegramParser(client, channels, storage)
    parser.run()
