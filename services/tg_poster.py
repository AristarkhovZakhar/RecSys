import sys
sys.path.append("../storage")

from typing import List, Dict, Union
from telethon.sync import TelegramClient


class Ranging:
    def __init__(self, model=None) -> None:
        self.model = model
        self._news = {}

    @property
    def get_news(self):
        return self._news

    def get_messages(
            self,
            labeler_results: List[Dict],
            summarizator_results: List[Dict],
            sort_by_score: bool = False
    ):
        self._get_messages_from_summarizer(summarizator_results)
        self._get_messages_from_labeler(labeler_results)

        # TODO: self.score_smth_with_model
        ranked_news = dict(
            sorted(self._news.items(), key=lambda item: item[1]['score'])
        ) if sort_by_score else self._news

        return ranked_news

    def _get_messages_from_summarizer(self, items: List[Dict]) -> None:
        print(items)
        for item in items:
            self._news[item['url']] = self._news.get(item['url'], {})
            self._news[item['url']]['text'] = item['summary']
            self._news[item['url']]['score'] = 0

    def _get_messages_from_labeler(self, items: List[Dict]) -> None:
        print(items)
        for item in items:
            self._news[item['url']]['labels'] = item['labels']

    def run_ranking_push_poster(self, **kwargs):
        ti = kwargs['ti']
        summary_items = ti.xcom_pull(task_ids='get_summary', key='summarizator for ranker')
        labeler_items = ti.xcom_pull(task_ids='get_labels', key='labeler for ranker')
        urls_news = self.get_messages(labeler_items, summary_items, sort_by_score=True)
        ti.xcom_push(key='unformatted messages for posting', value=urls_news)

        return urls_news


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

    def post_messages(self, news: Dict[str, Dict[str, float]]) -> None:
        for channel in self.channels:
            for url, value in news.items():
                self._client.send_message(
                    channel,
                    self._format_messages({'labels': value['labels'], 'text': value['text']})
                )

    def run_post_messages(self, **kwargs):
        ti = kwargs['ti']
        urls_news = ti.xcom_pull(task_ids='get_ranks', key='unformatted messages for posting')
        self.post_messages(urls_news)
