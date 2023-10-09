import sys
sys.path.append("../storage")

from typing import List, Dict, Any
from telethon.sync import TelegramClient


class TGPoster:
    def __init__(
            self,
            client: TelegramClient,
            channels: List[str],
            post_quantity: int = 5
    ):
        self._client = client
        self.channels = channels
        self.post_quantity = post_quantity

    def _format_messages(self, item: Dict[str, str]):
        message = f"<b><u>{' '.join(item['labels'])}</u></b>:\n\n"
        message += item['text']
        return message

    def post_messages(self, messages: List[Dict[str, Any]]) -> None:
        for m in messages:
            for channel in self.channels:
                self._client.send_message(
                    channel,
                    self._format_messages(
                        {
                            'labels': m['labels'],
                            'text': m['text']
                        }
                    )
                )

    def run_post_messages(self, **kwargs):
        ti = kwargs['ti']
        urls_news = ti.xcom_pull(task_ids='get_ranks', key='unformatted messages for posting')
        self.post_messages(urls_news)
