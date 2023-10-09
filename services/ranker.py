import random
from typing import List, Dict, Any
from airflow.models import Variable
from sentence_transformers import SentenceTransformer, util
import json

class Ranker:
    def __init__(self, tau=0.7, model_name: str = 'all-MiniLM-L6-v2'):
        self.labels = Variable.get('LABELS', default_var='[]', deserialize_json=True)
        self.tau = tau
        self.model = SentenceTransformer(model_name)
        self.scores = Variable.get('RANKER_SCORES', default_var='[]', deserialize_json=True)
        self.counts = Variable.get('RANKER_COUNTS', default_var='[]', deserialize_json=True)
        self.n_ranged_labels = 2
        self.top_news = 1
        self.prev_message = ''

    def get_valid_news(self, summary_items, labeler_items) -> List[Dict[str, str]]:
        messages = []
        for k in summary_items.keys():
            if not summary_items[k]['summary'] or not 'scores' in labeler_items[k].keys():
                continue
            messages.append(
                {
                    'summary': summary_items[k]['summary'],
                    'scores': {l: s for l, s in zip(labeler_items[k]['labels'], labeler_items[k]['scores'])}
                }
            )
        return messages

    def get_similarities(self, messages: List[Dict[str, str]]) -> List[float]:
        if not self.prev_message:
            return [0] * len(messages)
        prev_embedding = self.model.encode(self.prev_message)
        messages_embeddings = self.model.encode([m['summary'] for m in messages])
        cosine_scores = util.cos_sim(prev_embedding, messages_embeddings)[0].tolist()
        return cosine_scores

    def rank(self, messages: List[Dict[str, str]]) -> List[Dict[str, str]]:
        cosine_scores = self.get_similarities(messages)
        weights = [1] * len(list(self.counts.values())) if not any(list(self.counts.values())) else list(self.counts.values())
        ranged_labels = random.choices(self.labels, weights=weights, k=self.n_ranged_labels)
        for i, m in enumerate(messages):
            new_scores = {}
            for l in ranged_labels:
                new_scores[l] = (self.scores[l] + self.tau * self.counts[l] * m['scores'][l]) * (1 - cosine_scores[i])
            m['new_scores'] = new_scores
            m['aggregate_score'] = sum(new_scores.values())
        top_news = sorted(messages, key=lambda x: x['aggregate_score'], reverse=True)[:self.top_news]
        for new in top_news:
            for label, score in new['new_scores'].items():
                self.counts[label] += 1
                self.scores[label] += score
        Variable.set('RANKER_SCORES', self.scores)
        Variable.set('RANKER_COUNTS', self.counts)
        return random.choice(top_news)

    def run_ranking_push_poster(self, **kwargs):
        ti = kwargs['ti']
        summary_items = ti.xcom_pull(task_ids='get_summary', key='summarizator for ranker')
        labeler_items = ti.xcom_pull(task_ids='get_labels', key='labeler for ranker')
        messages = self.get_valid_news(summary_items, labeler_items)
        message_to_push = self.rank(messages)
        ti.xcom_push(key='unformatted messages for posting', value=message_to_push)
